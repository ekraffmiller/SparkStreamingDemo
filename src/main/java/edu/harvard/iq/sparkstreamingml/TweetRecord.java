/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

public class TweetRecord implements java.io.Serializable {

    @Override
    public String toString() {
        return "TweetRecord{" + "status =" + status + '}';
    }
      public TweetRecord(String status) {
          this.status = status;
      }
      public String status;

      public String getStatus() {
          return status;
      }

    
      public void setStatus(String status) {
          this.status = status;
      }

  }