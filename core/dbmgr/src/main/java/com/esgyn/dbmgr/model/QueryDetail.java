package com.esgyn.dbmgr.model;

import java.sql.Timestamp;
import java.util.HashMap;

public class QueryDetail {

  public String getQueryID() {
    return queryID;
  }

  public void setQueryID(String queryID) {
    this.queryID = queryID;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Timestamp getStartTime() {
    return startTime;
  }

  public void setStartTime(Timestamp startTime) {
    this.startTime = startTime;
  }

  public Timestamp getEndTime() {
    return endTime;
  }

  public void setEndTime(Timestamp endTime) {
    this.endTime = endTime;
  }

  public HashMap<String, Object> getMetrics() {
    return metrics;
  }

  public void setMetrics(HashMap<String, Object> metrics) {
    this.metrics = metrics;
  }

  public String getQueryText() {
    return queryText;
  }

  public void setQueryText(String queryText) {
    this.queryText = queryText;
  }

  private String queryID;
  private String status;
  private String userName;
  private Timestamp startTime;
  private Timestamp endTime;
  private String queryText;
  private HashMap<String, Object> metrics;
}
