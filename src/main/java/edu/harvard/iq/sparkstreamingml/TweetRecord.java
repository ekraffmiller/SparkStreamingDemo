/*
 * Java Bean for Tweet data
 */
package edu.harvard.iq.sparkstreamingml;

import java.sql.Timestamp;

public class TweetRecord implements java.io.Serializable {
    public String status;
    public Timestamp createdAt;

    @Override
    public String toString() {
        return "TweetRecord{" + "status =" + status + '}';
    }
      public TweetRecord(String status) {
          this.status = status;
      }
     public TweetRecord(String status, Timestamp createdAt) {
          this.status = status;
          this.createdAt=createdAt;
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

  }