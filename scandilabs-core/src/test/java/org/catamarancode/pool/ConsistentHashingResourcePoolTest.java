package org.catamarancode.pool;

import java.io.IOException;

import junit.framework.TestCase;

public class ConsistentHashingResourcePoolTest extends TestCase {

    public void testThreeNodePool() throws IOException {
        
        ConsistentHashingResourcePool pool = new ConsistentHashingResourcePool(3);
        
        long[] bucketCounts = new long[3];
        
        // Make sure keys get evenly allocated
        for (int i = 0; i < 99999; i++) {
            String key = "abcd:" + i + ":efgh";
            int bucket = pool.locateResource(key);
            bucketCounts[bucket]++;
        }
        
        for (int i = 0; i < 3; i++) {
            System.out.println("bucket " + i + ": " + bucketCounts[i]);    
        }        
    }
    
    public void testFourNodePool() throws IOException {
        
        ConsistentHashingResourcePool pool = new ConsistentHashingResourcePool(4);
        
        long[] bucketCounts = new long[4];
        
        // Make sure keys get evenly allocated
        for (int i = 0; i < 10000; i++) {
            String key = "abcd:" + i + ":efgh";
            int bucket = pool.locateResource(key);
            bucketCounts[bucket]++;
        }
        
        for (int i = 0; i < 4; i++) {
            System.out.println("bucket " + i + ": " + bucketCounts[i]);    
        }        
    }
    
    public void testTenNodePool() throws IOException {
        
        ConsistentHashingResourcePool pool = new ConsistentHashingResourcePool(10);
        
        long[] bucketCounts = new long[pool.getSize()];
        
        // Make sure keys get evenly allocated
        for (int i = 0; i < 1000000; i++) {
            String key = "abcd:" + i + ":efgh";
            int bucket = pool.locateResource(key);
            bucketCounts[bucket]++;
        }
        
        for (int i = 0; i < pool.getSize(); i++) {            
            System.out.println(pool.getBucketByIndex(i) + ": " + bucketCounts[i]);    
        }        
    }
}
