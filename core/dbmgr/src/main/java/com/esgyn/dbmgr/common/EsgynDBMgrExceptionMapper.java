// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.common;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class EsgynDBMgrExceptionMapper implements ExceptionMapper<EsgynDBMgrException> {

  public EsgynDBMgrExceptionMapper() {
  }

  @Override
  public Response toResponse(EsgynDBMgrException dbManagerException) {

    return Response.status(dbManagerException.getStatus()).entity(dbManagerException.getMessage())
        .type(MediaType.TEXT_PLAIN).build();
  }

}
