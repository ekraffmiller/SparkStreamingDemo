/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
/**
 *
 * @author ellenk
 */
public class CoreNLPExample {

    public static void main(String args[]) throws Exception {
        // Turn off logging so it's easier to see console output
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        if (args.length < 1) {
            System.err.println("Usage: CoreNLPExample <csv path> ");
            System.exit(1);
        }
        String csvPath = args[0];

        SparkSession session = SparkSession
                .builder()
                .appName("CoreNLP Example")
                .getOrCreate();
        String language = "english";
        Dataset<Row> docText =  loadCSVText(session, csvPath);
       // String serializedClassifier = "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";
  String serializedClassifier = "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz";
    //    
        // Test to view the annotations as an xml string
        StructType nlpSchema = new StructType();
        nlpSchema = nlpSchema.add("text", DataTypes.StringType);
        nlpSchema = nlpSchema.add("map", DataTypes.StringType);
       
        docText.map(row -> {
            String str = ((String) row.getAs("text"));
            String classified = LazyInstantiator.getClassifier(serializedClassifier).classifyToString(str,"xml",true);
          
            return RowFactory.create(                   
                    str,
                    classified
            );
        },RowEncoder.apply(nlpSchema)).select("map").show(false);
    
           
        //This gets the entity label and filters for non-O entities.
        // TODO: aggregate sequential entities 
        // TODO: get counts for entities and sort by most frequent
         docText.flatMap(row -> {
            List<List<CoreLabel>> labels = LazyInstantiator.getClassifier(serializedClassifier).classify((String) row.getAs("text"));
            List<Tuple2<String, String>> result = new ArrayList<>();
            for (List<CoreLabel> lcl : labels) {
                for (CoreLabel cl : lcl) {
                    result.add(new Tuple2(cl.word(), cl.get(CoreAnnotations.AnswerAnnotation.class)));
                }
            }
            return result.iterator();
        },
                Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF().filter(row -> !row.getAs("value").equals("O"))
                .show(500,false);
         
         
         // TRY POS!
              docText.flatMap(row -> {
            List<List<CoreLabel>> labels = LazyInstantiator.getClassifier(serializedClassifier).classify((String) row.getAs("text"));
            List<Tuple2<String, String>> result = new ArrayList<>();
            for (List<CoreLabel> lcl : labels) {
                for (CoreLabel cl : lcl) {
                    result.add(new Tuple2(cl.word(), cl.get(CoreAnnotations.PartOfSpeechAnnotation.class)));
                }
            }
            return result.iterator();
        },
                Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF();
         //       .show(500,false); 
        
        // Do Ner and pos in one pipeline
          // Add in sentiment
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos,lemma,ner");
    props.setProperty("ner.useSUTime", "0");
    props.setProperty("pos.model", "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
    props.setProperty("ner.model","/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz");
    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

      
    Dataset<Annotation> annotationDF = docText.map(row -> {
        Annotation an = new Annotation((String)row.getAs("text"));
       LazyInstantiator.getStanfordCoreNLP(props).annotate(an); 
    List<CoreMap> sentenceList = an.get(CoreAnnotations.SentencesAnnotation.class);
    if (sentenceList != null && ! sentenceList.isEmpty()) {
      CoreMap sentence = sentenceList.get(0);
      System.out.println("The keys of the first sentenceList's CoreMap are:");
      System.out.println(sentence.keySet());
      System.out.println();
      System.out.println("The first sentenceList is:");
      System.out.println(sentence.toShorterString());
      System.out.println();
      System.out.println("The first sentenceList tokens are:");
      for (CoreMap token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
         System.out.println(token.toShorterString());
        
      } 
    }return an;
    },Encoders.bean(Annotation.class));
    
    
    annotationDF.foreach(ann -> LazyInstantiator.getStanfordCoreNLP(props).annotate(ann));
    
    System.out.println("first annotation: "+ annotationDF.first());
    
    session.stop();
    
    
    
    }

    /**
     * Loads the csv file from the specified path using SparkContext.
     *
     * @param session -- Spark Session.
     * @param csvPath -- Absolute file path of to csv file. (we are assuming
     * it's text column is called "text")
     * @return -- Spark DataFrame of the csv file with text and docIndex columns
     */
    public static Dataset<Row> loadCSVText(SparkSession session, String csvPath) {

        Dataset<Row> docTextDF = session.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvPath)
                .toDF();

        // some abracts are null, so dont use them.
        return docTextDF
                .filter(row -> row.getAs("text") != null);
    }

}

/*
StructType(
StructField(docIndex,IntegerType,true),
StructField(Item Type,StringType,true), 
StructField(Publication Year,IntegerType,true), 
StructField(Author,StringType,true), 
StructField(Title,StringType,true), 
StructField(Publication Title,StringType,true), 
StructField(text,StringType,true), 
StructField(document,
    ArrayType(StructType(
            StructField(annotatorType,StringType,true), 
            StructField(begin,IntegerType,false), 
            StructField(end,IntegerType,false), 
            StructField(result,StringType,true), 
            StructField(metadata,MapType(StringType,StringType,true),true)),
            true),
    true), 
StructField(sentence,
    ArrayType(StructType(
        StructField(annotatorType,StringType,true), 
        StructField(begin,IntegerType,false), 
        StructField(end,IntegerType,false),
        StructField(result,StringType,true), 
        StructField(metadata,MapType(StringType,StringType,true),true)),
        true),
    true), 
StructField(token,
    ArrayType(StructType(
        StructField(annotatorType,StringType,true), 
        StructField(begin,IntegerType,false), 
        StructField(end,IntegerType,false), 
        StructField(result,StringType,true), 
        StructField(metadata,MapType(StringType,StringType,true),true))
        ,true),
    true)
)
*/