// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.filter;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import com.esgyn.dbmgr.resources.ConfigurationResource;

public class SessionListener implements HttpSessionListener {

  public SessionListener() {

  };

  public void sessionCreated(HttpSessionEvent event) {
    int sessionTimeOutMin = ConfigurationResource.getInstance().getSessionTimeoutMinutes();
    // setMaxInactiveInterval affects only the current session not the whole web server's
    // timeout.
    event.getSession().setMaxInactiveInterval(sessionTimeOutMin * 60);
  }

  public void sessionDestroyed(HttpSessionEvent event) {

  }

}
