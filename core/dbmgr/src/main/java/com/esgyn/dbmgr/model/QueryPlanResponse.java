// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.model;

import java.util.ArrayList;
import java.util.Hashtable;

public class QueryPlanResponse {
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Hashtable getData() {
    return data;
  }

  public void setData(Hashtable data) {
    this.data = data;
  }

  public ArrayList<QueryPlanResponse> getChildren() {
    return children;
  }

  public void setChildren(ArrayList<QueryPlanResponse> children) {
    this.children = children;
  }

  public String getPlanText() {
    return planText;
  }

  public void setPlanText(String planText) {
    this.planText = planText;
  }

  public int getTreeDepth() {
    return treeDepth;
  }

  public void setTreeDepth(int treeDepth) {
    this.treeDepth = treeDepth;
  }

  String id;
  String name;
  Hashtable data = new Hashtable();
  ArrayList<QueryPlanResponse> children = new ArrayList<QueryPlanResponse>();
  private String planText = "";
  private int treeDepth;
}
