package com.scandilabs.framework.solr;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scandilabs.framework.solr.SearchQuery;
import com.scandilabs.framework.solr.ShardedSolrServer;
import com.scandilabs.framework.solr.SolrServerConfig;

/**
 * Unit test for simple App.
 */
public class ShardedSolrServerTest extends TestCase {
    
    static Logger logger = LoggerFactory.getLogger(ShardedSolrServerTest.class
            .getName());
    
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public ShardedSolrServerTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ShardedSolrServerTest.class);
    }

    /**
     * Note that a local solr server must be running for this test to pass
     * @throws IOException 
     */
    public void testLoadByKey() throws IOException {
        
        List<SolrServerConfig> servers = new ArrayList<SolrServerConfig>();
        SolrServerConfig server0 = new SolrServerConfig();
        server0.setSolrHost("localhost");
        server0.setSolrPort(8999);
        server0.init();
        servers.add(server0);
        ShardedSolrServer shardedSolr = new ShardedSolrServer(servers);        
        String key = "WEBDOC-http://en.wikipedia.org/wiki/Silverton";
        try {
            shardedSolr.loadByKey(key);    
        } catch (SolrServerException e) {
            
            // Prevent a missing solr server from failing the build
            Throwable cause = e.getRootCause();
            if (cause instanceof ConnectException) {
                String msg = "***\n***\n*** TEST CASE FAILURE: Could not connect to Solr server\n***\n***";
                System.err.println(msg);
                logger.error(msg, cause);
            } else {
                throw new RuntimeException("Load exception", e);    
            }            
        }
        
        
    }
    
    @SuppressWarnings("deprecation")
	public void testClientShardedQuery() throws IOException {
        
        List<SolrServerConfig> servers = new ArrayList<SolrServerConfig>();
        SolrServerConfig server0 = new SolrServerConfig();
        server0.setSolrHost("50.28.67.228");
        server0.setSolrPort(8999);
        server0.init();
        
        SolrServerConfig server1 = new SolrServerConfig();
        server1.setSolrHost("50.28.72.53");
        server1.setSolrPort(8999);
        server1.init();
        
        servers.add(server0);
        servers.add(server1);
        
        Set<String> friendIds = new HashSet<String>();
        friendIds.add("201307556");
        friendIds.add("1378531");
        friendIds.add("23611635");
        friendIds.add("15394484");
        friendIds.add("20678329");
        friendIds.add("140827994");
        friendIds.add("17042139");
        friendIds.add("14075928");
        friendIds.add("21340015");
        friendIds.add("44273603");
        friendIds.add("24973756");
        friendIds.add("158414847");
        friendIds.add("74432340");
        friendIds.add("61898653");
        friendIds.add("813286");
        friendIds.add("176235525");
        friendIds.add("50393960");
        friendIds.add("20609518");
        friendIds.add("26071044");
        friendIds.add("37533");
        friendIds.add("44908387");
        friendIds.add("13851292");
        friendIds.add("28909306");
        friendIds.add("59142548");
        friendIds.add("18795216");
        friendIds.add("76986721");
        friendIds.add("14431692");
        friendIds.add("107190473");
        friendIds.add("197312437");
        friendIds.add("44030337");
        friendIds.add("19614344");
        friendIds.add("17220934");
        friendIds.add("17004139");
        friendIds.add("61135090");
        friendIds.add("157009365");
        friendIds.add("95674894");
        friendIds.add("208584178");
        friendIds.add("92451392");
        friendIds.add("149180925");

        ShardedSolrServer shardedSolr = new ShardedSolrServer(servers);        

        String term = "google";
        try {
        	SearchQuery searchQuery = new SearchQuery( String.format("status:(%s) OR web-title:(%s)", term, term, term) );
        	searchQuery.turnFacetOn(true, Arrays.asList("full-id"));
        	searchQuery.filterByField("source-user-id", friendIds);
			searchQuery.setRows(20L);
			searchQuery.getSolrQuery().setFacetSort(false);
			searchQuery.getSolrQuery().setFacetLimit(-1);
			searchQuery.setPagination(0L);
			
            QueryResponse response = shardedSolr.query(searchQuery.getSolrQuery(), SolrRequest.METHOD.GET);
            response.getResults().size();
        } catch (SolrServerException e) {
            throw new RuntimeException("Load exception", e);
        }
        
    }
    
}
