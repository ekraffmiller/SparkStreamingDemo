package edu.harvard.iq.sparkstreamingml;

import au.com.bytecode.opencsv.CSVReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;

public class EnhancedTweetRecord implements java.io.Serializable {

    public String status;
    public Timestamp createdAt;
    public Double prediction;

    @Override
    public String toString() {
        return "TweetRecord{" + "status =" + status + '}';
    }

    public EnhancedTweetRecord(String csvLine) {
        CSVReader reader = new CSVReader(new StringReader(csvLine));
        try {
            String[] vals = reader.readNext();
            prediction = new Double(vals[0]);
            status = vals[1];
            createdAt = Timestamp.valueOf(vals[2]);
        } catch (IOException e) {
            System.out.println("could not read as csv string: " + e.getMessage()+ csvLine);
        }
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Double getPrediction() {
        return prediction;
    }

    public void setPrediction(Double prediction) {
        this.prediction = prediction;
    }

}
