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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

public class OrcFileReader
{

    static Logger logger = Logger.getLogger(OrcFileReader.class.getName());;

    Configuration               m_conf;
    Path                        m_file_path;
    
    Reader                      m_reader;
    static Map<String, Reader>  s_map_readers = new HashMap<String, Reader>();

    List<OrcProto.Type>         m_types;
    StructObjectInspector       m_oi;
    List<? extends StructField> m_fields;
    RecordReader                m_rr;
    ByteOrder                   m_byteorder = ByteOrder.LITTLE_ENDIAN;
    VectorizedRowBatch          m_batch = null;
    boolean                     m_vector_mode = true;
    long                        m_first_row_in_batch;
    int                         m_curr_row_in_batch;

    String                      lastError = null;
    Reader.Options		m_options;
    boolean                     m_include_cols[];
    int                         m_col_count;
    byte[]                      m_row_ba;
    int                         m_max_rows_to_fill_in_block;

    public static class OrcRowReturnSQL
    {
	int m_row_length;
	int m_column_count;
	long m_row_number;
	byte[] m_row_ba = new byte[4096];
    }

    OrcRowReturnSQL		rowData; 

    /* Only to be used for testing - when called by this file's main() */
    static void setupLog4j() {
       System.out.println("In setupLog4J");
       String confFile = System.getenv("MY_SQROOT")
	   + "/conf/log4j.hdfs.config";
       System.setProperty("trafodion.hdfs.log", System.getenv("PWD") + "/org_trafodion_sql_OrcFileReader_main.log");
       PropertyConfigurator.configure(confFile);
    }

    OrcFileReader() 
    {
	if (logger.isTraceEnabled()) logger.trace("Enter OrcFileReader()");
	m_conf = new Configuration();
	rowData = new OrcRowReturnSQL();     
    }

    public String open(String pv_file_name,
		       int    pv_num_cols_to_project,
		       int[]  pv_which_cols) throws IOException 
    {

	if (logger.isDebugEnabled()) logger.debug("Enter open()," 
						  + " file name: " + pv_file_name
						  + " num_cols_to_project: " + pv_num_cols_to_project
						  + " pv_which_cols: " + pv_which_cols
						  );

	m_file_path = new Path(pv_file_name);
	
	m_reader = s_map_readers.get(pv_file_name);
	if (m_reader == null) {
	    if (logger.isDebugEnabled()) logger.debug("open() - creating a reader, path: " + pv_file_name);
	    try{
		m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf));
	    } catch (java.io.FileNotFoundException e1) {
		if (logger.isTraceEnabled()) logger.trace("Error: file not found: " + pv_file_name);
		return "file not found";
	    }
	    // commented out for the time being
	    //s_map_readers.put(pv_file_name, m_reader);
	    //if (logger.isDebugEnabled()) logger.debug("open() - put reader to map, for: "
	    //+ pv_file_name
	    //					      + " map size: "
	    //+ s_map_readers.size()
	    //);
	}

	if (logger.isDebugEnabled()) logger.debug("open() - reader created, path: " + pv_file_name);
	if (m_reader == null) {
	    if (logger.isTraceEnabled()) logger.trace("Error: open failed, createReader returned a null object");
	    return "open failed!";
	}

	if (logger.isTraceEnabled()) logger.trace("open() Reader opened, file name: " + pv_file_name);

	m_types = m_reader.getTypes();
	m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	m_fields = m_oi.getAllStructFieldRefs();
	if (logger.isTraceEnabled()) logger.trace("open() got MD types, file name: " + pv_file_name);

	int lv_num_cols_in_table = m_types.size();
	m_include_cols = new boolean[lv_num_cols_in_table];

	boolean lv_include_col = false;
	m_col_count = pv_num_cols_to_project;
	if (pv_num_cols_to_project == -1) {
	    lv_include_col = true;
	    m_col_count = m_fields.size();
	}
	
	// Initialize m_include_cols
	for (int i = 0; i < lv_num_cols_in_table; i++) {
	    m_include_cols[i] = lv_include_col;

	}

	// Set m_include_cols as per the passed in parameters
	if ((pv_num_cols_to_project > 0) &&
	    (pv_which_cols != null)) {
	    for (int lv_curr_index : pv_which_cols) {
		if ((lv_curr_index >= 1) &&
		    (lv_curr_index <= lv_num_cols_in_table)) {
		    m_include_cols[lv_curr_index] = true;
		}
	    }
	}

	if (logger.isDebugEnabled()) logger.debug("open() - before creating recordreader");
	try{
	    m_rr = m_reader.rowsOptions(new Reader.Options()
				    .include(m_include_cols)
				    );
	} catch (java.io.IOException e1) {
	    logger.error("reader.rows returned an exception: " + e1);
	    return (e1.getMessage());
	}
	
	if (logger.isDebugEnabled()) logger.debug("got a record reader, file name: " + pv_file_name);

	if (m_rr == null) {
	    if (logger.isTraceEnabled()) logger.trace("m_reader.rows() returned a null");
	    return "open:RecordReader is null";
	}

	m_row_ba = new byte[1024 * 64];
	m_max_rows_to_fill_in_block = 32;
	if (logger.isTraceEnabled()) logger.trace("Exit open()");
	return null;
    }
    
    boolean moreRowsInBatch()
    {
	if (m_curr_row_in_batch >= m_batch.size) {
	    return false;
	}
       
	return true;
    }

    void getNextBatchIfNeeded()
    {
	if (logger.isTraceEnabled()) logger.trace("getNextBatchIfNeeded() Enter."
						  );

	if (
	    (m_batch == null) ||
	    (m_curr_row_in_batch >= m_batch.size) 
	    ) {

	    if ((m_batch != null) &&
		(m_batch.endOfFile)) {
		if (logger.isTraceEnabled()) logger.trace("getNextBatchIfNeeded(),"
							  + " m_batch at endOfFile."
							  );
		m_batch = null;
		return;
	    }
	    
	    getNextBatch();
	}

    }

    void getNextBatch() {

	if (logger.isDebugEnabled()) logger.debug("getNextBatch() Enter, m_batch.endOfFile:"
						  + (m_batch == null ? "(null)" : m_batch.endOfFile )
						  );

	if (m_rr == null) {
	    return;
	}

	try {
	    m_first_row_in_batch = this.getPosition();
	}
	catch (java.io.IOException ejii) {
	    if (logger.isTraceEnabled()) logger.error("getNextBatch() this.getPosition() returned java.io.IOException: " 
						      + ejii);
	    m_batch = null;
	}
	if (m_batch != null) {
	    m_batch.reset();
	}
	m_curr_row_in_batch = 0;

	try {
	    m_batch = m_rr.nextBatch(m_batch);
	} 
	catch (java.io.IOException ejii) {
	    if (logger.isTraceEnabled()) logger.error("getNextBatch() reader.nextBatch returned java.io.IOException: " 
						      + ejii);
	    m_batch = null;
	}
	catch (java.lang.IndexOutOfBoundsException ejliob) {
	    if (logger.isTraceEnabled()) logger.trace("getNextBatch() reader.nextBatch returned java.lang.IndexOutOfBoundsException\n: "
						      + ejliob);
	    m_batch = null;
	}

	if (m_batch == null) {
	    if (logger.isTraceEnabled()) logger.debug("getNextBatch, m_batch is null");
	}
	else {
	    if (logger.isTraceEnabled()) logger.debug("getNextBatch, Vectorized row batch," 
						      + " size: " + m_batch.size
						      + " #cols: " + m_batch.cols.length
						      );
	}

    }

    public String close()
    {
	if (logger.isTraceEnabled()) logger.trace("Enter close()");
	m_reader = null;
	try {
	    m_rr.close(); 
	}
	catch (java.io.IOException e1) {
	    if (logger.isTraceEnabled()) logger.error("reader.rows returned an exception: " + e1);
	}
	m_file_path = null;            
	
	
	if (m_batch != null) {
	    m_batch.reset();
	}

	m_curr_row_in_batch = 0;

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
	
	if (logger.isTraceEnabled()) logger.trace("seekToRow, " 
						  + " to rowNumber: " + pv_rowNumber
						  + " next row returned: " + this.getPosition()
						  );

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

	if (logger.isTraceEnabled()) logger.trace("seeknSync, " 
						  + " to rowNumber: " + pv_rowNumber
						  + " next row returned: " + this.getPosition()
						  );

	if (logger.isTraceEnabled()) logger.trace("seeknSync, " 
						  + " to rowNumber: " + pv_rowNumber
						  + " next row returned after getNextBatch(): " + this.getPosition()
						  );


	return null;
    }

    public void setByteOrder(ByteOrder pv_bo) {
        m_byteorder = pv_bo;
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
	while (m_rr.hasNext()) {
	    System.out.println(fetchNextRow('|'));
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
	}
    }

    // returns the next row as a byte array
    public byte[] fetchNextRowOrig() throws Exception 
    {

	if (logger.isDebugEnabled()) logger.debug("Enter fetchNextRow()");

	if ( ! m_rr.hasNext()) {
	    return null;
	}

	OrcStruct lv_row = (OrcStruct) m_rr.next(null);
	Object lv_field_val = null;
   	ByteBuffer lv_row_buffer;

	byte[] lv_row_ba = new byte[4096];
	lv_row_buffer = ByteBuffer.wrap(lv_row_ba);
	lv_row_buffer.order(m_byteorder);

	lv_row_buffer.putInt(0);
	lv_row_buffer.putInt(m_col_count);
	lv_row_buffer.putLong(m_rr.getRowNumber());
	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length1: " + lv_row_buffer.position());

	for (int i = 0; i < m_fields.size(); i++) {
	    if (! m_include_cols[i+1]) continue;

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
	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length2: " + lv_row_buffer.position());
	lv_row_buffer.putInt(0, lv_row_buffer.position() - 16);
	return lv_row_buffer.array();
    }

    // returns the next row as a byte array
    public byte[] fetchNextRow() throws Exception 
    {

	if (logger.isDebugEnabled()) logger.debug("Enter fetchNextRow()");

	if ( ! m_rr.hasNext()) {
	    return null;
	}

	OrcStruct lv_row = (OrcStruct) m_rr.next(null);
	byte[] lv_row_ba = new byte[4096];

	int lv_filled_bytes = fillNextRow(lv_row_ba,
					  0,
					  lv_row);
	if (logger.isDebugEnabled()) logger.debug("fetchNextRow(), fillNextRow returned: " 
						  + lv_filled_bytes);

	return lv_row_ba;
    }

    // returns the next row as a byte array from the Vectorized buffer
    public byte[] fetchNextRowFromVector() throws Exception 
    {

	if (logger.isDebugEnabled()) logger.debug("Enter fetchNextRowFromVector()");

	getNextBatchIfNeeded();

	if (m_batch == null) {
	    return null;
	}

	byte[] lv_row_ba = new byte[4096];

	int lv_filled_bytes = fillNextRowFromVector(lv_row_ba,
						    0);

	if (logger.isDebugEnabled()) logger.debug("fetchNextRowFromVector()," 
						  + " fillNextRowFromVector returned: " 
						  + lv_filled_bytes);

	m_curr_row_in_batch++;
	return lv_row_ba;
    }

    // fills the row in the given byte[]
    public int fillNextRowFromVector(byte[]    p_row_ba, 
				     int       p_offset
				     ) throws Exception 
    {
	
	if (logger.isDebugEnabled()) logger.debug("Enter fillNextRowFromVector(),"
						  + " offset: " 
						  + p_offset
						  + " length of array: " 
						  + (p_row_ba == null ? 0 : p_row_ba.length)
						  );

	if ((p_row_ba  == null) ||
	    (m_batch == null)) {
	    return 0;
	}

	Object lv_field_val = null;
   	ByteBuffer lv_row_bb;

	lv_row_bb = ByteBuffer.wrap(p_row_ba,
				    p_offset,
				    p_row_ba.length - p_offset);
	lv_row_bb.order(m_byteorder);

	lv_row_bb.putInt(p_offset); // just a placeholder at this point to advance the pointer
	lv_row_bb.putInt(m_col_count);
	lv_row_bb.putLong(m_first_row_in_batch + m_curr_row_in_batch + 1);
	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length1: " + lv_row_bb.position());

	for (int i = 0; i < m_fields.size(); i++) {
	    if (! m_include_cols[i+1]) continue;

	    if (m_batch.cols[i].isNull[m_curr_row_in_batch]) {
  		lv_row_bb.putInt(0);
		continue;
	    }

	    lv_field_val = m_batch.cols[i].getWritableObject(m_curr_row_in_batch);
	    String lv_field_val_str = lv_field_val.toString();
	    lv_row_bb.putInt(lv_field_val_str.length());
	    if (lv_field_val != null) {
		lv_row_bb.put(lv_field_val_str.getBytes());
	    }
	}

	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length2: " + lv_row_bb.position());
	int lv_filled_bytes = lv_row_bb.position() - p_offset;
	lv_row_bb.putInt(p_offset, lv_filled_bytes - 16);

	return (lv_filled_bytes);
    }

    // fills the row in the given byte[]
    public int fillNextRow(byte[]    p_row_ba, 
			   int       p_offset,
			   OrcStruct p_orc_row
			   ) throws Exception 
    {
	
	if (logger.isDebugEnabled()) logger.debug("Enter fillNextRow(),"
						  + " offset: " 
						  + p_offset
						  + " length of array: " 
						  + (p_row_ba == null ? 0 : p_row_ba.length)
						  );

	if ((p_row_ba  == null) ||
	    (p_orc_row == null)) {
	    return 0;
	}

	Object lv_field_val = null;
   	ByteBuffer lv_row_bb;

	lv_row_bb = ByteBuffer.wrap(p_row_ba,
				    p_offset,
				    p_row_ba.length - p_offset);
	lv_row_bb.order(m_byteorder);

	lv_row_bb.putInt(p_offset); // just a placeholder at this point to advance the pointer
	lv_row_bb.putInt(m_col_count);
	lv_row_bb.putLong(m_rr.getRowNumber());
	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length1: " + lv_row_bb.position());

	for (int i = 0; i < m_fields.size(); i++) {
	    if (! m_include_cols[i+1]) continue;

	    lv_field_val = m_oi.getStructFieldData(p_orc_row, m_fields.get(i));
	    if (lv_field_val == null) {
  		lv_row_bb.putInt(0);
		continue;
	    }
	    String lv_field_val_str = lv_field_val.toString();
	    lv_row_bb.putInt(lv_field_val_str.length());
	    if (lv_field_val != null) {
		lv_row_bb.put(lv_field_val_str.getBytes());
	    }
	}

	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length2: " + lv_row_bb.position());
	int lv_filled_bytes = lv_row_bb.position() - p_offset;
	lv_row_bb.putInt(p_offset, lv_filled_bytes - 16);

	return (lv_filled_bytes);
    }

    /**************************************************
     *
     * Returns the next Block as a byte[]
     *
     * The Block structure is:
     * - int: number of Row Elements in the Block
     * - Array of Row Elements:
     *     - Each Row Element:
     *     - int: length of the Row
     *     - int: number of columns in the Row
     *     - long: Row number (in the table)
     *     - byte[]: the Row
     *         - Each Row:
     *         - int: length of the column
     *         - byte[]: column content
     *
     **************************************************/
    public byte[] fetchNextBlockFromVector() throws Exception 
    {
	if (logger.isDebugEnabled()) logger.debug("Enter fetchNextBlockFromVector()");

	getNextBatchIfNeeded();

	if (m_batch == null) {
	    return null;
	}

	int lv_num_rows = 0;
	int lv_row_offset = 4; // Initial offset to store the number of orc rows
	int lv_filled_bytes = 0;
	boolean lv_done = false;

	ByteBuffer lv_block_bb;
	lv_block_bb = ByteBuffer.wrap(m_row_ba,
				      0,
				      m_row_ba.length);
	lv_block_bb.order(m_byteorder);

	while ((moreRowsInBatch()) &&
	       (lv_num_rows < m_max_rows_to_fill_in_block) &&
	       (!lv_done)) {
	    if (logger.isTraceEnabled()) logger.trace("fetchNextBlockFromVector (in the loop):" 
						      + " numRows: " + lv_num_rows
						      + " row offset: " + lv_row_offset
						      + " filled bytes: " + lv_filled_bytes
						      );
	
	    lv_filled_bytes  = fillNextRowFromVector(m_row_ba,
						     lv_row_offset
						     );

	    m_curr_row_in_batch++;

	    if (lv_filled_bytes > 0) {
		lv_row_offset += lv_filled_bytes;
		lv_num_rows++;
	    }
	    else {
		lv_done = true;
	    }
	}
	
	if (logger.isTraceEnabled()) logger.trace("fetchNextBlockFromVector (out of the loop):" 
						  + " numRows: " + lv_num_rows
						  + " row offset: " + lv_row_offset
						  + " filled bytes: " + lv_filled_bytes
						  );
	// Set the number of rows in the block header
	lv_block_bb.putInt(0, lv_num_rows);

	return m_row_ba;
    }
	
    public byte[] fetchNextBlock() throws Exception 
    {
	if (logger.isDebugEnabled()) logger.debug("Enter fetchNextBlock()");

	if ( ! m_rr.hasNext()) {
	    return null;
	}

	int lv_num_rows = 0;
	int lv_row_offset = 4; // Initial offset to store the number of orc rows
	int lv_filled_bytes = 0;
	boolean lv_done = false;

	ByteBuffer lv_block_bb;
	lv_block_bb = ByteBuffer.wrap(m_row_ba,
				      0,
				      m_row_ba.length);
	lv_block_bb.order(m_byteorder);

	while ((m_rr.hasNext()) &&
	       (lv_num_rows < m_max_rows_to_fill_in_block) &&
	       (!lv_done)) {
	    if (logger.isTraceEnabled()) logger.trace("fetchNextBlock (in the loop):" 
						      + " numRows: " + lv_num_rows
						      + " row offset: " + lv_row_offset
						      + " filled bytes: " + lv_filled_bytes
						      );
	
	    OrcStruct lv_orc_row = (OrcStruct) m_rr.next(null);
	    lv_filled_bytes  = fillNextRow(m_row_ba,
					   lv_row_offset,
					   lv_orc_row);

	    if (lv_filled_bytes > 0) {
		lv_row_offset += lv_filled_bytes;
		lv_num_rows++;
	    }
	    else {
		lv_done = true;
	    }
	}
	
	if (logger.isTraceEnabled()) logger.trace("fetchNextBlock (out of the loop):" 
						  + " numRows: " + lv_num_rows
						  + " row offset: " + lv_row_offset
						  + " filled bytes: " + lv_filled_bytes
						  );
	// Set the number of rows in the block header
	lv_block_bb.putInt(0, lv_num_rows);

	return m_row_ba;
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
	lv_row_buffer.order(m_byteorder);
	
	rowData.m_row_length = 0;
	rowData.m_column_count = m_col_count;
	rowData.m_row_number = m_rr.getRowNumber();
	
	for (int i = 0; i < m_fields.size(); i++) {
	    if (! m_include_cols[i+1]) continue;

	    lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
	    if (lv_field_val == null) {
  		lv_row_buffer.putInt(0);
  		rowData.m_row_length = rowData.m_row_length + lv_integerLength;
		continue;
	    }
	    String lv_field_val_str = lv_field_val.toString();
	    lv_row_buffer.putInt(lv_field_val_str.length());
	    rowData.m_row_length += lv_integerLength;

	    lv_row_buffer.put(lv_field_val_str.getBytes());
	    rowData.m_row_length += lv_field_val_str.length();
	}

	if (logger.isTraceEnabled()) logger.trace("Bytebuffer length: " + lv_row_buffer.position());
	return rowData;
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
	    if (! m_include_cols[i+1]) continue;

	    lv_field_val = m_oi.getStructFieldData(lv_row, m_fields.get(i));
	    if (lv_field_val != null) {
		lv_row_string.append(lv_field_val);
	    }
	    lv_row_string.append(pv_ColSeparator);
	}
	
	return lv_row_string.toString();
    }

    public String fetchNextRowFromVector(char pv_ColSeparator) throws Exception 
    {

	if (logger.isTraceEnabled()) logger.trace("Enter fetchNextRowFromVector()," 
						  + " col separator: " + pv_ColSeparator);

	getNextBatchIfNeeded();

	if (m_batch == null) {
	    return null;
	}

	if (logger.isTraceEnabled()) logger.trace("fetchNextRowFromVector()," 
						  + " next row to be returned: " + m_curr_row_in_batch
						  + " m_batch.endOfFile: " + m_batch.endOfFile
						  );

	Object lv_field_val = null;
   	StringBuilder lv_row_string = new StringBuilder(1024);

	for (int i = 0; i < m_batch.cols.length; i++) {
	    if (! m_include_cols[i+1]) continue;

	    if (! m_batch.cols[i].isNull[m_curr_row_in_batch] )  {
		lv_field_val = m_batch.cols[i].getWritableObject(m_curr_row_in_batch);
		lv_row_string.append(lv_field_val);
	    }
	    lv_row_string.append(pv_ColSeparator);
	}
	
	m_curr_row_in_batch++;
	return lv_row_string.toString();
    }

    String getLastError() 
    {
	return lastError;
    }

    void setVectorMode(boolean pv_mode)
    {
	m_vector_mode = pv_mode;
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

    // test push-down with ORC
    public String selectiveScan(String pv_file_name) throws IOException {

	m_file_path = new Path(pv_file_name);

	try{
          m_reader = OrcFile.createReader(m_file_path, OrcFile.readerOptions(m_conf));
	} catch (java.io.FileNotFoundException e1) {
          return "file not found";
	}
	if (m_reader == null)
		return "open failed!";
	m_types = m_reader.getTypes();
	m_oi = (StructObjectInspector) m_reader.getObjectInspector();
	m_fields = m_oi.getAllStructFieldRefs();

        // hive 0.13.1
        //SearchArgument.Builder builder = SearchArgument.FACTORY.newBuilder();
        //
        // hive 1.1
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();

        // other possible predicates
//.startAnd().equals("_col2", "2").end()
//.startOr().equals("_col2", 2).equals("_col1", 3).end()
//.startNot().equals("_col2", "2").end()
//.startAnd().between("_col2", 2, 3).end()
//.startAnd().in("_col2", 1, 3, 4).end()
        SearchArgument sarg = builder
        .startAnd().in("_col2", 1, 3, 4).end()
        .build();
	
        // test 
	//boolean include[] = {true, true, true};
        //String colNames[] = {null, "_col0", "_col1"};
        //
	boolean[] include = new boolean[1+m_fields.size()];
	String[] colNames = new String[1+m_fields.size()];

        colNames[0] = null;
	include[0] = true;

        for (int i = 1; i <= m_fields.size(); i++) {
	   include[i] = true;
	   colNames[i] = new String("_col" + i);
        }

	try{
	   m_rr = m_reader.rowsOptions(new Reader.Options()
                    .range(0L, Long.MAX_VALUE)
                    .include(include)
                    .searchArgument(sarg, colNames));

	} catch (java.io.IOException e1) {
           return (e1.getMessage());
	}
	
        long startRowNum = m_rr.getRowNumber();
	System.out.println("<<<<<< RecorderReader: " + m_rr + ", startRowNum=" + startRowNum);

	if (m_rr == null)
           return "open:RecordReader is null";

        Object field_val = null;
        StringBuilder row_string = new StringBuilder(1024);
        int ct = 0;
        OrcStruct row = null;

        while (m_rr.hasNext() ) {
            startRowNum = m_rr.getRowNumber();
            //System.out.println("To read a row with rowNum=" + startRowNum);
            ct++;
            row = (OrcStruct) m_rr.next(row);
            row_string.setLength(0);
            for (int i = 0; i < m_fields.size(); i++) {
                field_val = m_oi.getStructFieldData(row, m_fields.get(i));
                if (field_val != null) {
                    row_string.append(field_val);
                }
                row_string.append('|');
            }
            System.out.println(row_string);
        }

	return null;
    }

    public static void mainOld(String[] args) throws Exception
    {
	boolean lv_print_info = false;
	boolean lv_perform_scans = false;
        boolean lv_perform_selective_scans = false;
        boolean lv_perform_vectorized_scans = false;

	setupLog4j();

	for (String lv_arg : args) {
	    if (lv_arg.compareTo("-i") == 0) {
		lv_print_info = true;
	    }
	    if (lv_arg.compareTo("-s") == 0) {
		lv_perform_scans = true;
	    }
	    if (lv_arg.compareTo("-ss") == 0) {
		lv_perform_selective_scans = true;
	    }
	    if (lv_arg.compareTo("-vs") == 0) {
		lv_perform_vectorized_scans = true;
	    }
	}

	System.out.println("OrcFile Reader main");

	OrcFileReader lv_this = new OrcFileReader();

	int lv_include_cols [] = new int[4];
	lv_include_cols[0]=1;
	lv_include_cols[1]=2;
	lv_include_cols[2]=6;
	lv_include_cols[3]=7;
	lv_this.open(args[0], 4, lv_include_cols);

	if (lv_print_info) {
	    System.out.println("================= Begin File Info:" + 
			       args[0]);
	
	    lv_this.printFileInfo();

	    System.out.println("================= End File Info:" + 
			       args[0]);
	}

	if (lv_perform_scans) {
	    System.out.println("================= Begin: readFile_ByteBuffer");
	    lv_this.readFile_ByteBuffer();
	    System.out.println("================= End: readFile_ByteBuffer");

	    System.out.println("================= Begin: readFile_String");
	    lv_this.readFile_String();
	    System.out.println("================= End: readFile_String");

	    System.out.println("================= Begin: seeknSync(8)");
	    boolean lv_done = false;
	    if (lv_this.seeknSync(8) == null) {
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

	    lv_done = false;
	    String lv_row_string;
	    if (lv_this.seeknSync(11) == null) {
		System.out.println("================= Begin: After seeknSync(11)... will do fetchNextRow('|')");
		while (! lv_done) {
		    lv_row_string = lv_this.fetchNextRow('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}
		System.out.println("================= End: After seeknSync(11)... will do fetchNextRow('|')");
	    }

	    lv_this.close();
	    lv_this.open(args[0], -1, null);

	    if (lv_this.seeknSync(10) == null) {
		System.out.println("================= Begin: After seeknSync(11)... will do fetchNextRowObj()");
		lv_done = false;
		while (! lv_done) {
		    OrcRowReturnSQL lv_sql_obj = lv_this.fetchNextRowObj();
		    if (lv_sql_obj == null) {
			lv_done = true;
		    }
		}
		System.out.println("================= End: After seeknSync(11)... will do fetchNextRowObj()");
	    }

	    lv_this.close();
	    lv_this.open(args[0], -1, null);

	    lv_done = false;
	    if (lv_this.seeknSync(8) == null) {
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

	    lv_done = false;
	    if (lv_this.seeknSync(0) == null) {
		System.out.println("================= Begin: fetchNextBlock()");
		System.out.println("Next row #: " + lv_this.getPosition());
		byte[] lv_block_ba = lv_this.fetchNextBlock();
		if (lv_block_ba != null) {
		    ByteBuffer lv_block_bb = ByteBuffer.wrap(lv_block_ba);
		    int lv_num_rows = lv_block_bb.getInt();
		    System.out.println("Length lv_block_bb: " + lv_block_ba.length);
		    System.out.println("Number of rows in the block: " + lv_num_rows);
		    System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_block_ba, 0, 100));
		}

		System.out.println("================= End: fetchNextBlock()");
	    }

	}
        else if ( lv_perform_selective_scans ) {
	    System.out.println(lv_this.selectiveScan(args[1]));
        }
    }

    public static void main(String[] args) throws Exception
    {
	boolean lv_print_info = false;
	boolean lv_perform_scans = false;
        boolean lv_perform_selective_scans = false;
        boolean lv_perform_vectorized_scans = false;
	boolean lv_done = false;

	setupLog4j();

	for (String lv_arg : args) {
	    if (lv_arg.compareTo("-i") == 0) {
		lv_print_info = true;
	    }
	    if (lv_arg.compareTo("-s") == 0) {
		lv_perform_scans = true;
	    }
	    if (lv_arg.compareTo("-ss") == 0) {
		lv_perform_selective_scans = true;
	    }
	    if (lv_arg.compareTo("-vs") == 0) {
		lv_perform_vectorized_scans = true;
	    }
	}

	System.out.println("OrcFile Reader main");

	OrcFileReader lv_this = new OrcFileReader();
	lv_this.setByteOrder(ByteOrder.BIG_ENDIAN);

	int lv_include_cols [] = new int[4];
	lv_include_cols[0]=1;
	lv_include_cols[1]=2;
	lv_include_cols[2]=6;
	lv_include_cols[3]=7;
	lv_this.open(args[0], 4, lv_include_cols);

	if (lv_print_info) {
	    System.out.println("================= Begin File Info:" + 
			       args[0]);
	
	    lv_this.printFileInfo();

	    System.out.println("================= End File Info:" + 
			       args[0]);
	}

	if (lv_perform_scans) {
	    lv_done = false;
	    String lv_row_string;
	    lv_this.setVectorMode(false);
	    if (lv_this.seeknSync(1) == null) {
		logger.trace("================= Begin: After seeknSync(1)... will do fetchNextRow('|')");
		while (! lv_done) {
		    lv_row_string = lv_this.fetchNextRow('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}
		logger.trace("================= End: After seeknSync(1)... will do fetchNextRow('|')");
	    }

	    lv_this.close();
	    lv_this.open(args[0], -1, null);

	    lv_done = false;
	    if (lv_this.seeknSync(8) == null) {
		logger.trace("================= Begin: After seeknSync(8)...fetchNextRowFromVector('|')");
		while (! lv_done) {
		    lv_row_string = lv_this.fetchNextRow('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}
	    }

	    lv_this.close();
	    lv_this.open(args[0], -1, null);

	    lv_done = false;
	    if (lv_this.seeknSync(8) == null) {
		logger.trace("================= Begin: fetchNextRow()");
		while (! lv_done) {
		    System.out.println("Next row #: " + lv_this.getPosition());
		    byte[] lv_row_ba = lv_this.fetchNextRow();
		    if (lv_row_ba != null) {
			ByteBuffer lv_row_bb = ByteBuffer.wrap(lv_row_ba);
			System.out.println("Length of the returned byte array: " + lv_row_ba.length 
					   + " data[] len: " + lv_row_bb.getInt()
					   + " col count: " + lv_row_bb.getInt()
					   + " row number: " + lv_row_bb.getLong());
			System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_ba, 0, 100));
		    }
		    else {
			lv_done = true;
		    }
		}
		logger.trace("================= End: byte[] fetchNextRow()");
	    }

	    lv_done = false;
	    if (lv_this.seeknSync(0) == null) {
		logger.trace("================= Begin: fetchNextBlock()");
		System.out.println("Next row #: " + lv_this.getPosition());
		byte[] lv_block_ba = lv_this.fetchNextBlock();
		if (lv_block_ba != null) {
		    ByteBuffer lv_block_bb = ByteBuffer.wrap(lv_block_ba);
		    System.out.println("Length of the returned byte array: " + lv_block_ba.length 
				       + " #rows in the block: " + lv_block_bb.getInt()
				       + " data[] len: " + lv_block_bb.getInt()
				       + " col count: " + lv_block_bb.getInt()
				       + " row number: " + lv_block_bb.getLong()
				       + " 1st col len: " + lv_block_bb.getInt());
		    System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_block_ba, 0, 100));
		}

		logger.trace("================= End: fetchNextBlock()");
	    }

	}
	else if (lv_perform_vectorized_scans) {
	    lv_done = false;
	    String lv_row_string;
	    lv_this.setVectorMode(true);
	    if (lv_this.seeknSync(1) == null) {
		logger.trace("================= Begin: After seeknSync(1)... will do fetchNextRowFromVector('|')");
		while (! lv_done) {
		    lv_row_string = lv_this.fetchNextRowFromVector('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}
		logger.trace("================= End: After seeknSync(1)... will do fetchNextRowFromVector('|')");
	    }

	    lv_this.close();
	    lv_this.open(args[0], -1, null);

	    lv_done = false;
	    if (lv_this.seeknSync(8) == null) {
		logger.trace("================= Begin: After seeknSync(8)...fetchNextRowFromVector('|')");
		while (! lv_done) {
		    lv_row_string = lv_this.fetchNextRowFromVector('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }
		}

		lv_done = false;
		while (! lv_done) {
		    System.out.println("Next row #: " + lv_this.getPosition());

		    lv_row_string = lv_this.fetchNextRowFromVector('|');
		    if (lv_row_string != null) {
			System.out.println(lv_row_string);
		    }
		    else {
			lv_done = true;
		    }

		}
		logger.trace("================= End: fetchNextRowFromVector('|')");
	    }

	    lv_done = false;
	    if (lv_this.seeknSync(8) == null) {
		logger.trace("================= Begin: byte[] fetchNextRowFromVector()");
		while (! lv_done) {
		    System.out.println("Next row #: " + lv_this.getPosition());
		    byte[] lv_row_ba = lv_this.fetchNextRowFromVector();
		    if (lv_row_ba != null) {
			ByteBuffer lv_row_bb = ByteBuffer.wrap(lv_row_ba);
			System.out.println("Length of the returned byte array: " + lv_row_ba.length 
					   + " data[] len: " + lv_row_bb.getInt()
					   + " col count: " + lv_row_bb.getInt()
					   + " row number: " + lv_row_bb.getLong());
			System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_row_ba, 0, 100));
		    }
		    else {
			lv_done = true;
		    }
		}
		logger.trace("================= End: byte[] fetchNextRowFromVector()");
	    }
	    
	    lv_done = false;
	    if (lv_this.seeknSync(0) == null) {
		logger.trace("================= Begin: fetchNextBlockFromVector()");
		System.out.println("Next row #: " + lv_this.getPosition());
		byte[] lv_block_ba = lv_this.fetchNextBlockFromVector();
		if (lv_block_ba != null) {
		    ByteBuffer lv_block_bb = ByteBuffer.wrap(lv_block_ba);
		    System.out.println("Length of the returned byte array: " + lv_block_ba.length 
				       + " #rows in the block: " + lv_block_bb.getInt()
				       + " data[] len: " + lv_block_bb.getInt()
				       + " col count: " + lv_block_bb.getInt()
				       + " row number: " + lv_block_bb.getLong()
				       + " 1st col len: " + lv_block_bb.getInt());
		    System.out.println("First 100 bytes of lv_row_bb: " + new String(lv_block_ba, 0, 100));
		}

		logger.trace("================= End: fetchNextBlockFromVector()");
	    }


	}
        else if ( lv_perform_selective_scans ) {
	    System.out.println(lv_this.selectiveScan(args[1]));
        }
    }
}
