/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ellenk
 */
public class NLPExampleTest {
    
    public NLPExampleTest() {
    }

    /**
     * Test of main method, of class NLPExample.
     */
    @Test
    public void testMain() throws Exception {
      String csvPath = "/Users/ellenk/scratch/landmarkSets/Gary\\ King\\ Abstracts/GaryKingAbstractsNotesWithMetadata.csv"; 
        System.setProperty("spark.master","local[2]" );   
    
         NLPExample.main(new String[]{csvPath});
    }

    /**
     * Test of loadCSVText method, of class NLPExample.
     */
    @Test
    public void testLoadCSVText() {
    }
    
}
