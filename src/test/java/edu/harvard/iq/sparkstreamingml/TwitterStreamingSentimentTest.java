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
public class TwitterStreamingSentimentTest {
    
    public TwitterStreamingSentimentTest() {
    }

    /**
     * Test of main method, of class TwitterStreamingSentiment.
     * @throws java.lang.InterruptedException
     */
    @org.junit.Test
    public void testMain() throws InterruptedException {
        System.setProperty("spark.master","local[2]");   
       
        String consumerKey = "FrJYpexihWGIS9eeTyOlWGVvn";
        String consumerSecret = "y4bgXB1wjInhwk5dDfUBaRKx4G05m7SdqIH2mLqbhAQMiyFaMV";
        String accessToken = "35462430-8jz6rQdcarZCarELF94LsPaumaytzN9FRNItmEYde";
        String accessSecret = "Bt6u1CQuzrbX0lHEterUem0dpn3XpyGaAA2NixiwXYFJC";
        String modelPath = "/Users/ellenk/src/SparkStreamingML/data/naiveBayes";
       
        String[] args = {consumerKey,consumerSecret,accessToken,accessSecret,modelPath};
    
        TwitterStreamingSentiment.main(args);
    }

  
    
}
