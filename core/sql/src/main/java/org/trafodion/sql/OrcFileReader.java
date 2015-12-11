// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.sql;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.ql.io.orc.*;

import org.apache.log4j.Logger;

public class OrcFileReader
{

    static Logger logger = Logger.getLogger(OrcFileReader.class.getName());;

    Configuration               m_conf;
    Path                        m_file_path;
    
    Reader                      m_reader;
    List<OrcProto.Type>         m_types;
    StructObjectInspector       m_oi;
    List<? extends StructField> m_fields;
    RecordReader                m_rr;
    String                      lastError = null;
    Reader.Options		m_options;

    public class OrcRowReturnSQL
    {
	int m_row_length;
	int m_column_count;
	long m_row_number;
	byte[] m_row_ba = new byte[4096];
    }

    OrcRowReturnSQL		rowData;	//TEMP!!

    OrcFileReader() 
    {
	if (logger.isTraceEnabled()) logger.trace("Enter OrcFileReader()");
	m_conf = new Configuration();
	rowData = new OrcRowReturnSQL();	//TEMP: was in fetch
    }

    public String open(String pv_file_name) throws IOException 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter open(), file name: " + pv_file_name);

	m_file_path = new Path(pv_file_name);

	try{
	    m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf));
	} catch (java.io.FileNotFoundException e1) {
	    if (logger.isTraceEnabled()) logger.trace("Error: file not found: " + pv_file_name);
	    return "file not found";
	}

	if (m_reader == null) {
	    if (logger.isTraceEnabled()) logger.trace("Error: open failed, createReader returned a null object");
	    return "open failed!";
	}

	if (logger.isTraceEnabled()) logger.trace("open() Reader opened, file name: " + pv_file_name);
	m_types = m_reader.getTypes();
	m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	m_fields = m_oi.getAllStructFieldRefs();
	if (logger.isTraceEnabled()) logger.trace("open() got MD types, file name: " + pv_file_name);
	
	try{
	    m_rr = m_reader.rows();
	} catch (java.io.IOException e1) {
	    logger.error("reader.rows returned an exception: " + e1);
	    return (e1.getMessage());
	}
	
	if (m_rr == null) {
	    if (logger.isTraceEnabled()) logger.trace("m_reader.rows() returned a null");
	    return "open:RecordReader is null";
	}

	if (logger.isTraceEnabled()) logger.trace("Exit open()");
	return null;
    }
    
    public String close()
    {
	if (logger.isTraceEnabled()) logger.trace("Enter close()");
	m_reader = null;
	m_rr = null; 
	m_file_path = null;            
	return null;
    }

    public void printFileInfo() throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter printFileInfo()");
	
	if (m_reader == null) {
	    if (logger.isTraceEnabled()) logger.trace("OrcFileReader.printFileInfo: Error: reader object is null. Exitting");
	    return;
	}

	System.out.println("Reader: " + m_reader);

	System.out.println("# Rows: " + m_reader.getNumberOfRows());
	System.out.println("Size of the file (bytes): " + m_reader.getContentLength());
	System.out.println("# Types in the file: " + m_types.size());
	for (int i=0; i < m_types.size(); i++) {
	    System.out.println("Type " + i + ": " + m_types.get(i).getKind());
	}

	System.out.println("Compression: " + m_reader.getCompression());
	if (m_reader.getCompression() != CompressionKind.NONE) {
	    System.out.println("Compression size: " + m_reader.getCompressionSize());
	}

	m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	
	System.out.println("object inspector type category: " + m_oi.getCategory());
	System.out.println("object inspector type name    : " + m_oi.getTypeName());

	System.out.println("Number of columns in the table: " + m_fields.size());

	// Print the type info:
	for (int i = 0; i < m_fields.size(); i++) {
	    System.out.println("Column " + i + " name: " + m_fields.get(i).getFieldName());
	    ObjectInspector lv_foi = m_fields.get(i).getFieldObjectInspector();
	    System.out.println("Column " + i + " type category: " + lv_foi.getCategory());
	    System.out.println("Column " + i + " type name: " + lv_foi.getTypeName());
	}

    }

    public boolean seekToRow(long pv_rowNumber) throws IOException 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter seekToRow(), rowNumber: " + pv_rowNumber);

	if (m_reader == null) {
	    return false;
	}

	if ((pv_rowNumber < 0) ||
	    (pv_rowNumber >= m_reader.getNumberOfRows())) {
	    return false;
	}

	m_rr.seekToRow(pv_rowNumber);

	return true;
    }

    public String seeknSync(long pv_rowNumber) throws IOException 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter seeknSync(), rowNumber: " + pv_rowNumber);

	if (m_reader == null) {
	    if (logger.isTraceEnabled()) logger.trace("seeknSync(). Error: No reader object yet");
	    return "Looks like a file has not been opened. Call open() first.";
	}

	if ((pv_rowNumber < 0) ||
	    (pv_rowNumber >= m_reader.getNumberOfRows())) {
	    if (logger.isTraceEnabled()) logger.trace("seeknSync(). Error: Invalid row number");
	    return "Invalid rownumber: " + pv_rowNumber + " provided.";
	}

	m_rr.seekToRow(pv_rowNumber);

	return null;
    }

    public long getNumberOfRows() throws IOException 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter getNumberOfRows");

	return m_reader.getNumberOfRows();

    }

    public long getPosition() throws IOException 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter getPosition");

	return m_rr.getRowNumber();

    }

    // Dumps the content of the file. The columns are '|' separated.
    public void readFile_String() throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter readFile_String");

	if (m_rr == null) {
	    if (logger.isTraceEnabled()) logger.trace("readFile_String: Error: reader object is null. Exitting.");
	    return;
	}

	seeknSync(0);
	OrcStruct lv_row = null;
	Object lv_field_val = null;
   	StringBuilder lv_row_string = new StringBuilder(1024);
	while (m_rr.hasNext()) {
	    lv_row = (OrcStruct) m_rr.next(lv_row);
	    lv_row_string.setLength(0);
	    for (int i = 0; i < m_fields.size(); i++) {
		lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
		if (lv_field_val != null) {
		    lv_row_string.append(lv_field_val);
		}
		lv_row_string.append('|');
	    }
	    System.out.println(lv_row_string);
	}

    }

    // Dumps the contents of the file as ByteBuffer.
    public void readFile_ByteBuffer() throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter readFile_ByteBuffer()");

	OrcStruct lv_row = null;
	Object lv_field_val = null;
   	ByteBuffer lv_row_buffer;

	seeknSync(0);
	while (m_rr.hasNext()) {
	    byte[] lv_row_ba = new byte[4096];
	    lv_row_buffer = ByteBuffer.wrap(lv_row_ba);
	    lv_row = (OrcStruct) m_rr.next(lv_row);
	    for (int i = 0; i < m_fields.size(); i++) {
		lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
		if (lv_field_val == null) {
		    lv_row_buffer.putInt(0);
		    continue;
		}
		String lv_field_val_str = lv_field_val.toString();
		lv_row_buffer.putInt(lv_field_val_str.length());
		if (lv_field_val != null) {
		    lv_row_buffer.put(lv_field_val_str.getBytes());
		}
	    }
	    System.out.println(lv_row_buffer);
	    //	    System.out.println(new String(lv_row_buffer.array()));
	}
    }

    public String getNext_String(char pv_ColSeparator) throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter getNext_String()");

	if ( ! m_rr.hasNext()) {
	    return null;
	}

	OrcStruct lv_row = null;
	Object lv_field_val = null;
   	StringBuilder lv_row_string = new StringBuilder(1024);

	lv_row = (OrcStruct) m_rr.next(lv_row);
	for (int i = 0; i < m_fields.size(); i++) {
	    lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
	    if (lv_field_val != null) {
		lv_row_string.append(lv_field_val);
	    }
	    lv_row_string.append(pv_ColSeparator);
	}
	
	return lv_row_string.toString();
    }

    // returns the next row as a byte array
    public byte[] fetchNextRow() throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter fetchNextRow()");

	if ( ! m_rr.hasNext()) {
	    return null;
	}

	OrcStruct lv_row = (OrcStruct) m_rr.next(null);
	Object lv_field_val = null;
   	ByteBuffer lv_row_buffer;

	byte[] lv_row_ba = new byte[4096];
	lv_row_buffer = ByteBuffer.wrap(lv_row_ba);
	for (int i = 0; i < m_fields.size(); i++) {
	    lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
	    if (lv_field_val == null) {
  		lv_row_buffer.putInt(0);
		continue;
	    }
	    String lv_field_val_str = lv_field_val.toString();
	    lv_row_buffer.putInt(lv_field_val_str.length());
	    if (lv_field_val != null) {
		lv_row_buffer.put(lv_field_val_str.getBytes());
	    }
	}
	return lv_row_buffer.array();
    }
	
    public OrcRowReturnSQL fetchNextRowObj() throws Exception
    {

	if (logger.isTraceEnabled()) logger.trace("Enter fetchNextRowObj()");

	int	lv_integerLength = 4;
	 
	if ( ! m_rr.hasNext()) {
	    return null;
	}

	OrcStruct lv_row = (OrcStruct) m_rr.next(null);
	Object lv_field_val = null;
   	ByteBuffer lv_row_buffer;

	lv_row_buffer = ByteBuffer.wrap(rowData.m_row_ba);
	lv_row_buffer.order(ByteOrder.LITTLE_ENDIAN);
	
	rowData.m_row_length = 0;
	rowData.m_column_count = m_fields.size();
	rowData.m_row_number = m_rr.getRowNumber();
	
	for (int i = 0; i < m_fields.size(); i++) {
	    lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
	    if (lv_field_val == null) {
  		lv_row_buffer.putInt(0);
  		rowData.m_row_length = rowData.m_row_length + lv_integerLength;
		continue;
	    }
	    String lv_field_val_str = lv_field_val.toString();
	    lv_row_buffer.putInt(lv_field_val_str.length());
	    rowData.m_row_length = rowData.m_row_length + lv_integerLength;
	    if (lv_field_val != null) {
		lv_row_buffer.put(lv_field_val_str.getBytes());
  		rowData.m_row_length = rowData.m_row_length + lv_field_val_str.length();
	    }
	}
    	 
	return rowData;
	
    }

    String getLastError() 
    {
	return lastError;
    }

    public boolean isEOF() throws Exception
    { 
	if (logger.isTraceEnabled()) logger.trace("Enter isEOF()");

	if (m_rr.hasNext()) {
	    return false;
	}
	else {
	    return true;
	}
    }  

    public String fetchNextRow(char pv_ColSeparator) throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter fetchNextRow(), col separator: " + pv_ColSeparator);

	if ( ! m_rr.hasNext()) {
	    return null;
	}

	OrcStruct lv_row = null;
	Object lv_field_val = null;
   	StringBuilder lv_row_string = new StringBuilder(1024);

	lv_row = (OrcStruct) m_rr.next(lv_row);
	for (int i = 0; i < m_fields.size(); i++) {
	    lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
	    if (lv_field_val != null) {
		lv_row_string.append(lv_field_val);
	    }
	    lv_row_string.append(pv_ColSeparator);
	}
	
	return lv_row_string.toString();
    }

    public static void main(String[] args) throws Exception
    {
	boolean lv_print_info = false;
	boolean lv_perform_scans = false;

	for (String lv_arg : args) {
	    if (lv_arg.compareTo("-i") == 0) {
		lv_print_info = true;
	    }
	    if (lv_arg.compareTo("-s") == 0) {
		lv_perform_scans = true;
	    }
	}

	System.out.println("OrcFile Reader main");

	OrcFileReader lv_this = new OrcFileReader();

	lv_this.open(args[0]);

	if (lv_print_info) {
	    System.out.println("================= Begin File Info:" + 
			       args[0]);
	
	    lv_this.printFileInfo();

	    System.out.println("================= End File Info:" + 
			       args[0]);
	}

	if (lv_perform_scans) {
	    System.out.println("================= Begin: readFile_String");
	    lv_this.readFile_String();
	    System.out.println("================= End: readFile_String");

	    System.out.println("================= Begin: readFile_ByteBuffer");
	    lv_this.readFile_ByteBuffer();
	    System.out.println("================= End: readFile_ByteBuffer");

	    System.out.println("================= Begin: seeknSync(4)");
	    // Gets rows as byte[]  (starts at row# 4)
	    boolean lv_done = false;
	    if (lv_this.seeknSync(4) == null) {
		System.out.println("================= Begin: fetchNextRow()");
		while (! lv_done) {
		    System.out.println("Next row #: " + lv_this.getPosition());
		    byte[] lv_row_bb = lv_this.fetchNextRow();
		    if (lv_row_bb != null) {
			System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_bb, 0, 100));
			System.out.println("Length lv_row_bb: " + lv_row_bb.length);
		    }
		    else {
			lv_done = true;
		    }
		}
		System.out.println("================= End: fetchNextRow()");
	    }

	    // Gets rows as String (starts at row# 10)
	    lv_done = false;
	    String lv_row_string;
	    if (lv_this.seeknSync(10) == null) {
		System.out.println("================= Begin: After seeknSync(10)... will do getNext_String()");
		while (! lv_done) {
		    lv_row_string = lv_this.getNext_String('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}
		System.out.println("================= End: After seeknSync(10)... will do getNext_String()");
	    }
	}

    }
}
