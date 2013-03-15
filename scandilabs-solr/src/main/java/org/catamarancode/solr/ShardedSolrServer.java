package org.catamarancode.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.catamarancode.pool.ConsistentHashingResourcePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardedSolrServer {

    private AtomicLong lastCommitTime = new AtomicLong();

    public static String facetDelimiter = "\t";

    static Logger logger = LoggerFactory.getLogger(ShardedSolrServer.class
            .getName());

    private List<SolrServerConfig> servers = new ArrayList<SolrServerConfig>();

    private ConsistentHashingResourcePool resourcePool;

    // A pool of threads to facilitate concurrent Solr requests,
    // thereby providing for client-side federation. 
    private ExecutorService threadPool;
    
    public ShardedSolrServer(List<SolrServerConfig> servers) {
        this.servers = servers;
        this.threadPool = Executors.newCachedThreadPool();
        resourcePool = new ConsistentHashingResourcePool(servers.size());
        logger.debug(String.format("Created SharderSolrServer with %d servers",
                servers.size()));
    }

    /**
     * Perform a Solr query and do custom, client-side federation to make it faster.
     * @param solrQuery
     * @param method
     * @return
     * @throws SolrServerException
     */
    public QueryResponse query(SolrQuery solrQuery, SolrRequest.METHOD method) throws SolrServerException {
    	return query(solrQuery, method, null, null);
    }
    
	public QueryResponse query(SolrQuery solrQuery, SolrRequest.METHOD method, Long firstResultId, Long firstResultDate)
            throws SolrServerException {
    	
    	long enter = new Date().getTime();
    	int rowLimit = 20;
    	int rowStart = 0;
    	int timeouts = 0;
    	String rows = "rows=(\\d+)";
    	Pattern pattern = Pattern.compile(rows);
    	Matcher matcher = pattern.matcher(solrQuery.toString());
    	if (matcher.find()) {
    		rowLimit= Integer.valueOf(matcher.group(1));
    		logger.debug("Row limit = " + rowLimit);
    	}
    	
    	String start = "start=(\\d+)";
    	pattern = Pattern.compile(start);
    	matcher = pattern.matcher(solrQuery.toString());
    	if (matcher.find()) {
    		rowStart = Integer.valueOf(matcher.group(1));
    		logger.debug("Row start = " + rowStart);
    	}
    	
    	// In order to implement correct paging behavior, always start from result 0
    	// and get more rows than requested (buffer that guards against duplicate or
    	// combined tweet/webdoc results.)
    	solrQuery.setRows(rowStart+(3*rowLimit));
    	solrQuery.setStart(0);
    	
        logger.debug(String.format("SHARD: Executing query %s", solrQuery.toString()));
        
        // Execute query on all servers concurrently.
        List<FutureTask<QueryResponse>> futures = new ArrayList<FutureTask<QueryResponse>>();
    	for (int i = 0; i < servers.size(); i++) {
    		SolrServerConfig serverConfig = servers.get(i);
    		FutureTask<QueryResponse> query = new FutureTask<QueryResponse>(new SingleShardQuery(serverConfig, solrQuery, method));
    		futures.add(query);
    		threadPool.execute(query);
    		logger.debug(String.format("SHARD: Scheduled query on shard %s", serverConfig.getSolrHost()));
    	}
    	
    	// Now wait for all responses to come back - future.get() blocks
    	// for up to the specified number of seconds.
    	List<QueryResponse> responses = new ArrayList<QueryResponse>();
    	for (FutureTask<QueryResponse> future : futures) {
    		try {
    			responses.add( future.get(10, TimeUnit.SECONDS) );
    		}
    		catch (TimeoutException e) {
    			logger.info("Query timeout");
    			timeouts++;
    		}
    		catch (ExecutionException e) {
    			logger.info("Execution exception", e);
    		}
    		catch (InterruptedException e) {
    			logger.info("Interrupted exception", e);
    		}
    	}
    	
    	logger.debug("SHARD: All responses returned.");
    	
    	// Once all are back, aggregate/sort the results and facets
    	QueryResponse aggregateResponse = federate(responses, rowStart, rowLimit, firstResultId, firstResultDate, (timeouts == servers.size()));
    	
    	long exit = new Date().getTime();
    	long delta = exit - enter;
    	logger.debug(String.format("SHARD: Finished combining results...Done in %d ms!", delta));
    	
        return aggregateResponse;
    }

    public QueryResponse query(SearchQuery searchQuery)
            throws SolrServerException {
        SolrQuery solrQuery = searchQuery.getSolrQuery();
        return query(solrQuery, searchQuery.getSolrMethod());
    }

    public QueryResponse query(SearchQuery searchQuery, Long firstResultId, Long firstResultDate)
	    	throws SolrServerException {
    	SolrQuery solrQuery = searchQuery.getSolrQuery();
    	return query(solrQuery, searchQuery.getSolrMethod(), firstResultId, firstResultDate);
	}
    
    /**
     * Adds a batch of solr documents to solr TODO: Maybe add per-server
     * batching later. If so, use a public commit() method call to signal the
     * end of an operation. For now, just add one-by-one
     * 
     * @param docs
     * @throws SolrServerException
     * @throws IOException
     */
    public void add(Collection<SolrInputDocument> docs)
            throws SolrServerException, IOException {
        logger.debug(String.format("Adding %d docs to sharded solr servers",
                docs.size()));
        for (SolrInputDocument doc : docs) {
            this.add(doc);
        }
    }

    /**
     * Adds a solr document to one of the sharded solr index servers, as
     * determined by which server the document 'key' field hashes to
     * 
     * @param doc
     * @throws SolrServerException
     * @throws IOException
     */
    public UpdateResponse add(SolrInputDocument doc)
            throws SolrServerException, IOException {
        SolrInputField keyField = doc.getField("key");
        String key = (String) keyField.getValue();
        int keyHashCode = resourcePool.computeKeyHash(key);
        int serverIndex = resourcePool.locateResourceByKeyHash(keyHashCode);

        // Add the keyHashCode to the solr document before saving, in case we
        // ever need to migrate docs for a keyHash range from one server to
        // another
        doc.setField("keyHashCode", keyHashCode);

        SolrServerConfig solr = this.servers.get(serverIndex);
        SolrServer server = solr.getSolrServer();
        UpdateResponse response = server.add(doc);
        logger.debug(String.format(
                "Added doc with key %s to server %s with response %s", key,
                solr.toString(), response.toString()));
        return response;
    }

    /**
     * Load one solr document by unique key
     * 
     * @param searchQuery
     * @return
     * @throws SolrServerException
     */
    public SolrDocument loadByKey(String key) throws SolrServerException {
        SearchQuery searchQuery = new SearchQuery("key:\"" + key + "\"");
        int keyHashCode = resourcePool.computeKeyHash(key);
        int serverIndex = resourcePool.locateResourceByKeyHash(keyHashCode);
        SolrServerConfig solr = this.servers.get(serverIndex);

        logger.debug(String.format("Looking up doc with key %s from server %d",
                key, serverIndex));
        QueryResponse response = solr.getSolrServer().query(
                searchQuery.getSolrQuery(), searchQuery.getSolrMethod());
        SolrDocumentList documentList = response.getResults();
        SolrDocument document = null;
        if (documentList.size() > 0) {
            document = documentList.get(0);
            logger.debug(String.format("Response from lookup for key %s : %s",
                    key, document.toString()));
        } else {
            logger.debug(String.format("No document found for key %s", key));
        }
        return document;
    }
    
    private final long MIN_DELAY_BETWEEN_MANUAL_COMMITS = 5000;

    /**
     * Manually commit docs added so far to all solr servers
     */
    public void commitToAllServers() {
        
        // Enough time elapsed since last commit?
        if ((System.currentTimeMillis() - lastCommitTime.longValue()) < MIN_DELAY_BETWEEN_MANUAL_COMMITS) {
            logger.info(String.format("Not enough time since last commit, skipping commit to all %d solr servers", servers
                    .size()));
            return;
        }
        lastCommitTime.set(System.currentTimeMillis());
        
        logger.debug(String.format("Committing to all %d solr servers", servers
                .size()));

        // Start commits at the same time
        Thread[] committerThreads = new Thread[servers.size()];
        for (int i = 0; i < servers.size(); i++) {
            Committer committer = new Committer(servers.get(i));
            committerThreads[i] = new Thread(committer);
            committerThreads[i].setName(String.format("Committer-%d", i));
            committerThreads[i].setDaemon(true);
            committerThreads[i].start();
        }

        // Wait for last commit to finish
        for (int i = 0; i < servers.size(); i++) {
            try {
                committerThreads[i].join();
            } catch (InterruptedException ie) {
                logger
                        .warn(
                                "Interrupted while waiting for committer thread to die",
                                ie);
            }

        }
        logger.debug(String.format("Committed to all %d solr servers", servers
                .size()));

    }

    /**
     * Aggregate query responses from n servers and combine into one response
     * @param responses
     * @param rowStart
     * @param rowLimit
     * @return
     */
    @SuppressWarnings("unchecked")
    private QueryResponse federate(List<QueryResponse> responses, int rowStart, int rowLimit, Long firstResultId, Long firstResultDate, boolean timeout) {
    	// Data structures to hold the combined facets and results.
    	Map<String, SolrDocument> allDocs = new HashMap<String, SolrDocument>();
    	Map<String, Long> allFullIdFacets = new HashMap<String, Long>();
    	Map<String, Long> allMetaFacets = new HashMap<String, Long>();
    	
    	// Iterate through every document/facet from every server - use of
    	// set avoids duplicates...
    	for (QueryResponse response : responses) {
    		if (response != null) {
	    		logger.debug(String.format("SHARD: Aggregating results for a shard..."));
	    		
	    		NamedList<Object> resp = response.getResponse();
	    		for( int i=0; i<resp.size(); i++ ) {
	    			String n = resp.getName( i );
	    			if ("response".equals(n)) {
	    				SolrDocumentList docs = (SolrDocumentList) resp.getVal(i);
	    				
	    				// Add these docs one at a time, because if there are duplicates,
	    				// we want to explicitly favor the one with the more recent
	    				// timestamp.  (Why? Because it might have meta information.)
	    				for (SolrDocument doc : docs) {
	    					String key = (String)doc.getFieldValue("key");
	    					
	    					if (allDocs.containsValue(key)) {
	    						
	    						SolrDocument first = allDocs.get(key);
	    						String firstTimestamp = (String)first.getFieldValue("timestamp");
	    						String docTimestamp = (String)doc.getFieldValue("timestamp");
	    						
	    						if (docTimestamp.compareTo(firstTimestamp) == 1) {
	    							allDocs.remove(key);
	    							allDocs.put(key, doc);
	    						}

	    					}
	    					else {
	    						allDocs.put(key, doc);
	    					}
	    				}
	    				
	    			}
	    			else if ("facet_counts".equals(n)) {
	    				NamedList<Object> info = (NamedList<Object>) resp.getVal(i);
	    				
	    			    NamedList<NamedList<Number>> ff = (NamedList<NamedList<Number>>) info.get( "facet_fields" );
	    			    if (ff != null) {
	
	    			    	for( Map.Entry<String,NamedList<Number>> facet : ff ) {
	    			    		String key = facet.getKey();
	    			    		if (key.equals("full-id")) {
	    	    			        for (Map.Entry<String, Number> entry : facet.getValue()) {
	    	    			        	String entryKey = entry.getKey();
	    	    			        	if (allFullIdFacets.containsKey(entryKey)) {
	    	    			        		Long count = allFullIdFacets.get(entryKey);
	    	    			        		count+= entry.getValue().longValue();
	    	    			        		allFullIdFacets.put(entryKey, count);
	    	    			        	}
	    	    			        	else {
	    	    			        		allFullIdFacets.put(entryKey, entry.getValue().longValue());
	    	    			        	}
	    	      			        }
	    			    		}
	    			    		else if (key.equals("meta")) {
	    	    			        for (Map.Entry<String, Number> entry : facet.getValue()) {
	    	    			        	String entryKey = entry.getKey();
	    	    			        	if (allMetaFacets.containsKey(entryKey)) {
	    	    			        		Long count = allMetaFacets.get(entryKey);
	    	    			        		count+= entry.getValue().longValue();
	    	    			        		allMetaFacets.put(entryKey, count);
	    	    			        	}
	    	    			        	else {
	    	    			        		allMetaFacets.put(entryKey, entry.getValue().longValue());
	    	    			        	}
	    	      			        }
	    			    		}
	    			    		
	    			    	}
	    			    }
	    			}
	    		}
    		}
    		else {
    			logger.info("Null response from solr!");
    		}
    	}
    	
    	// Create a list of documents and sort by creation date
    	List<SolrDocument> uniqueList = new ArrayList<SolrDocument>(allDocs.values());
    	Collections.sort(uniqueList, new SolrDocumentCreationComparator());
    	
    	logger.debug("SHARD: allDocs size: " + allDocs.size());
    	
    	// Construct and return the combined result
    	QueryResponse aggregateResponse = new QueryResponse();
    	NamedList<Object> res = new NamedList<Object>();
    	
    	// This is where we loop through all results returned from Solr, and choose
    	// the subset which corresponds to the client request (i.e. start at row 10,
    	// 5 results...)  Because some requests are continuations of previous ones,
    	// (page 3 of recent photos for example), and because new results may have
    	// shown up in between requests for page 2 and page 3, we have to make sure to
    	// use the same starting result to ensure continuity. Use tweetId as primary
    	// identifier, with timestamp as fall back.
    	SolrDocumentList combined = new SolrDocumentList();
    	int rowCount = 0;
    	boolean foundFirstResult = (firstResultId == null || firstResultDate == null);
    	
    	for (SolrDocument doc : uniqueList) {
    		
    		if (!foundFirstResult) {
    			logger.debug( String.format("Haven't found first result yet. first id,date = [%s, %s]", firstResultId, firstResultDate) );
    			Long tweetId = (Long)doc.getFieldValue("twitter-status-id");
    			Date timestamp = (Date)doc.getFieldValue("created");
    			if (tweetId == firstResultId) {
    				logger.debug(String.format("Found first result by id! first id,date = [%s, %s]", firstResultId, firstResultDate));
    				foundFirstResult = true;
    			}
    			else if (timestamp.getTime() < firstResultDate) {
    				logger.debug(String.format("Found first result by date! first id,date = [%s, %s]", firstResultId, firstResultDate));
    				foundFirstResult = true;
    			}
    			
    			if (!foundFirstResult) {
    				logger.debug("Didn't find first result. Moving on to next doc...");
    				continue;
    			}

    		}
    		
    		if (rowCount >= rowStart && rowCount < (rowStart+rowLimit)) {
    			combined.add(doc);
    			logger.debug("Added document: " + doc.getFieldValue("created").toString());
    		}
    		else {
    			logger.debug("Skipped document: " + doc.getFieldValue("created").toString());
    		}
    		
    		rowCount++;
    		
    		if (combined.size() == rowLimit) {
    			break;
    		}
    		
    	}
    	combined.setNumFound(allDocs.size());
    	res.add("response", combined);
    	
    	// Now do the facets...
    	NamedList<Object> combinedFacetCounts = new NamedList<Object>();
    	NamedList<NamedList<Number>> combinedFacetFields = new NamedList<NamedList<Number>>();
    	
    	// full-id
    	NamedList<Number> combinedFullIdFacetFieldValues = new NamedList<Number>();
    	for (Map.Entry<String, Long> entry : allFullIdFacets.entrySet()) {
    		combinedFullIdFacetFieldValues.add(entry.getKey(), entry.getValue());
    	}
    	combinedFacetFields.add("full-id", combinedFullIdFacetFieldValues);

    	// meta
    	NamedList<Number> combinedMetaFacetFieldValues = new NamedList<Number>();
    	for (Map.Entry<String, Long> entry : allMetaFacets.entrySet()) {
    		combinedMetaFacetFieldValues.add(entry.getKey(), entry.getValue());
    	}
    	combinedFacetFields.add("meta", combinedMetaFacetFieldValues);
    	
    	combinedFacetCounts.add("facet_fields", combinedFacetFields);
    	
    	res.add("facet_counts", combinedFacetCounts);
    	
    	NamedList<Object> responseHeader = new NamedList<Object>();
    	responseHeader.add("timeout", timeout);
    	res.add("responseHeader", responseHeader);
    	
    	aggregateResponse.setResponse(res);

    	return aggregateResponse;
    }
    
    private class Committer implements Runnable {

        private SolrServerConfig solrConfig;

        Committer(SolrServerConfig solrConfig) {
            this.solrConfig = solrConfig;
        }

        public void run() {
            logger.info(String.format("Committing to %s", solrConfig));
            try {

                // Commit. Note that we're using waitFlush and
                // waitSearcher so we'll wait until slowest server can be searched
                solrConfig.getSolrServer().commit(true, true);
                logger.info(String.format("Committed to %s", solrConfig));
            } catch (SolrServerException e) {
            	logger.error(e.toString());
                logger.error(String.format("Exception committing to solr server %s", solrConfig), e);
            } catch (IOException e) {
                logger.error(String.format("Exception committing to server %s", solrConfig), e);
            }
        }
    }
    
    /**
     * A Callable class that runs a single query on a single server and returns the response.
     * @author cstolte
     *
     */
    private class SingleShardQuery implements Callable<QueryResponse> {
        private SolrServerConfig solrConfig;
        private SolrQuery solrQuery;
        private SolrRequest.METHOD requestMethod;
        
        SingleShardQuery(SolrServerConfig config, SolrQuery query, SolrRequest.METHOD method) {
            this.solrConfig = config;
            this.solrQuery = query;
            this.requestMethod = method;
        }

        public QueryResponse call() {
        	
            logger.debug(String.format("Executing query on shard %s", solrConfig.getSolrHost()));
            
            QueryResponse response = null;
            try {
            	SolrServer server = solrConfig.getSolrServer();
            	response = server.query(solrQuery, requestMethod);
            	
            	logger.debug("QTime: " + response.getQTime());
                logger.debug(String.format("Got response from shard %s in %d ms", solrConfig.getSolrHost(), response.getElapsedTime()));
            } catch (SolrServerException e) {
                logger.error(String.format(
                        "Exception querying solr server %s", solrConfig), e);
            }
            
            return response;
        }
    }
    
    /**
     * Orders SolrDocuments in reverse created order (default for PostPost).
     * @author cstolte
     *
     */
    private class SolrDocumentCreationComparator implements Comparator<SolrDocument> {
	    public int compare(SolrDocument obj1, SolrDocument obj2) {
	    	Date created1 = (Date)obj1.getFieldValue("created");
	    	Date created2 = (Date)obj2.getFieldValue("created");
	    	
	    	// Ordered descending...
	    	return (created2.compareTo(created1));
	    }
    }
}
