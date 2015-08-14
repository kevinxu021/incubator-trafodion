package com.esgyn.dbmgr.resources;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import com.esgyn.dbmgr.common.EsgynDBMgrExceptionMapper;

@ApplicationPath("/")
public class EsgynDBMgrApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {

    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(QueryResource.class);
    classes.add(DCSResource.class);
    classes.add(LoginResource.class);
    classes.add(DatabaseResource.class);
    classes.add(WorkloadsResource.class);
    classes.add(LogsResource.class);
    classes.add(EsgynDBMgrExceptionMapper.class);
    classes.add(OpenTSDBResource.class);
    return classes;
  }
}
