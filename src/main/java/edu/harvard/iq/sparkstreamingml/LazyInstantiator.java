/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import java.util.Properties;

/**
 *
 * @author ellenk
 */
public class LazyInstantiator {
    private static CRFClassifier<CoreLabel> classifier = null;
    private static StanfordCoreNLP stanfordCoreNLP = null;
    public static CRFClassifier<CoreLabel> getClassifier(String path) throws Exception {
        if (classifier ==null) {
              System.out.println("classifier is null, assigning");
              classifier = CRFClassifier.getClassifier(path);
        }
        return classifier;
  
    }
    public static StanfordCoreNLP getStanfordCoreNLP(Properties props) throws Exception {
        if (stanfordCoreNLP ==null) {
              System.out.println("stanford is null, assigning");
              stanfordCoreNLP = new StanfordCoreNLP(props);
        }
        return stanfordCoreNLP;
  
    }

}
