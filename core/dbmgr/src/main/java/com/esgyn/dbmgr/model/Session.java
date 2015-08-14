package com.esgyn.dbmgr.model;

import org.joda.time.DateTime;

import com.esgyn.dbmgr.common.DateTimeSerializer;
import com.esgyn.dbmgr.resources.ConfigurationResource;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class Session {
  private String username;
  private String password;
  private DateTime date;

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @JsonSerialize(using = DateTimeSerializer.class)
  public DateTime getDate() {
    return date;
  }

  public void setDate(DateTime date) {
    this.date = date;
  }

  public Session(String username, String password, DateTime date) {
    this.username = username;
    this.password = password;
    this.date = date;
  }

  public static boolean isExpired(Session content) {
    int sessionTimeoutMinutes = ConfigurationResource.getInstance().getSessionTimeoutMinutes();
    if (sessionTimeoutMinutes < 10) // Minimum supported value is 10 minutes
      sessionTimeoutMinutes = 120; // if less than minimum set to the default which is 120 minutes

    long threshold = sessionTimeoutMinutes * 60 * 1000; // session timeout in milliseconds
    long elapsedMilliSecond = (System.currentTimeMillis() - content.getDate().getMillis());

    boolean isExpired = (elapsedMilliSecond > threshold) ? true : false;
    return isExpired;
  }

}
