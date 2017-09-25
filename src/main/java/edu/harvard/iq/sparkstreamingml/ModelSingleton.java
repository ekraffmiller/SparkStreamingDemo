package edu.harvard.iq.sparkstreamingml;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.NaiveBayesModel;


/**
 *
 * @author ellenk
 */
public class ModelSingleton {
    private static transient PipelineModel instance = null;
    public static PipelineModel getInstance(SparkConf sparkConf, String modelPath) {
        if (instance == null) {
            // Trigger the instantiation of the SparkSession before getting 
            // the model, so that the load method doesn't create one.  
            // (We can have only one SparkSession instance per JVM)
            SparkSessionSingleton.getInstance(sparkConf);
            instance = PipelineModel.load(modelPath);
        }
     
        return instance;
    }
}
