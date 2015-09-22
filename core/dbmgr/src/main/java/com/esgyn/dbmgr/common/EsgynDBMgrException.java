// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.common;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class EsgynDBMgrException extends Exception {

  private static final long serialVersionUID = 1L;
  private Response.Status status = Status.INTERNAL_SERVER_ERROR;

  public EsgynDBMgrException() {
    super("Unexpected exception");
  }

  public EsgynDBMgrException(String message) {
    super(message);
  }

  public EsgynDBMgrException(Status status, String message) {
    super(message);
    setStatus(status);
  }

  public EsgynDBMgrException(String message, Throwable cause) {
    super(message, cause);
  }

  public EsgynDBMgrException(Status status, String message, Throwable cause) {
    super(message, cause);
    setStatus(status);
  }

  public Response.Status getStatus() {
    return status;
  }

  private void setStatus(Response.Status status) {
    this.status = status;
  }

}
