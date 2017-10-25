/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 *
 * @author ellenk
 */
public class StemmingExampleTest {
    
    public StemmingExampleTest() {
      Logger.getLogger("org.apache").setLevel(Level.OFF);
     }

    /**
     * Test of main method, of class StemmingExample.
     */
    @Test
    public void testMain() throws Exception {
        String csvPath = "/Users/ellenk/scratch/landmarkSets/Gary\\ King\\ Abstracts/GaryKingAbstractsNotesWithMetadata.csv"; 
        System.setProperty("spark.master","local[2]" );   
    
         StemmingExample.main(new String[]{csvPath});
    }

    /**
     * Test of loadSentiment140File method, of class StemmingExample.
     */
    @Test
    public void testLoadSentiment140File() {
    }

    /**
     * Test of getDocFeatures method, of class StemmingExample.
     */
    @Test
    public void testGetDocFeatures() {
    }

    /**
     * Test of getDocStems method, of class StemmingExample.
     */
    @Test
    public void testGetDocStems() {
    }

    /**
     * Test of getNGramEntityValues method, of class StemmingExample.
     */
    @Test
    public void testGetNGramEntityValues() {
    }

    /**
     * Test of processDocuments method, of class StemmingExample.
     */
    @Test
    public void testProcessDocuments() {
       System.setProperty("spark.master","local[2]");   
     //  String mongoSetId = "59666ccd1d87fa8329cc547c";
       String mongoSetId= "59ea3bfe8cbe9713fa9fbe72";
       ObjectId classifierId = new ObjectId();
       System.out.println("classifierId = "+ classifierId.toHexString());
       StemmingExample.processDocuments(mongoSetId, classifierId.toHexString());
    }

    /**
     * Test of analyzeText method, of class StemmingExample.
     */
    @Test
    public void testAnalyzeText() {
    }

    /**
     * Test of loadCSVText method, of class StemmingExample.
     */
    @Test
    public void testLoadCSVText() {
    }
    
}
