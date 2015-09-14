package com.esgyn.dbmgr.common;

import java.util.ArrayList;

public class TabularResult {
	public boolean isScalarResult;
	public long timestamp;
	public String[] columnNames;
	public ArrayList<Object[]> resultArray;
}
