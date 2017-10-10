/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

/**
 *
 * @author ellenk
 */
public class HashTagsTest {
    
    public HashTagsTest() {
    }

    /**
     * Test of main method, of class TwitterStreamingSentiment.
     * @throws java.lang.InterruptedException
     */
    @org.junit.Test
    public void testMain() throws InterruptedException {
         
        String consumerKey = "mykey";
        String consumerSecret = "secret";
        String accessToken = "35462430-token";
        String accessSecret = "secret";
        System.setProperty("spark.master","local[2]");   
        String modelPath = "/Users/ellenk/src/SparkStreamingML/data/naiveBayes";
       
        String[] args = {consumerKey,consumerSecret,accessToken,accessSecret,modelPath};
        
        HashTags.main(args);
        
    }

   
}
