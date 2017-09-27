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
public class CreateNaiveBayesModelTest {
    
    public CreateNaiveBayesModelTest() {
    }

    /**
     * Test of main method, of class CreateNaiveBayesModel.
     */
    @org.junit.Test
    public void testMain() {
        System.setProperty("spark.master", "local[4]"); 
        String sentiment140Path = "/Users/ellenk/Downloads/trainingandtestdata_2/training.1600000.processed.noemoticon.csv";
        String modelPath = "/Users/ellenk/src/SparkStreamingML/data/naiveBayes";
       
      
    
        CreateNaiveBayesModel.main(new String[] {sentiment140Path,modelPath});
    }

   
    
}
