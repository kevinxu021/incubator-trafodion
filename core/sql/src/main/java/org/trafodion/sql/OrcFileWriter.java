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
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type.Kind;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.Integer;
import java.lang.Long;
import java.sql.Timestamp;
import java.sql.Date;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

public class OrcFileWriter {

    static Logger logger = Logger.getLogger(OrcFileWriter.class.getName());;
    
    Configuration conf;
    Path          filePath;
    Writer        writer;

    StandardStructObjectInspector sloi;
    List<String> listOfColName;  
    List<ObjectInspector> listOfObjectInspector; 

    NullWritable nullValue;

    // this set of constants MUST be kept in sync with 
    // datatype defines in common/dfs2rec.h
    private static final int REC_BIN16_SIGNED   = 130;
    private static final int REC_BIN16_UNSIGNED = 131;
    private static final int REC_BIN32_SIGNED   = 132;
    private static final int REC_BIN32_UNSIGNED = 133;
    private static final int REC_BIN64_SIGNED   = 134;
    private static final int REC_IEEE_FLOAT32   = 142;
    private static final int REC_IEEE_FLOAT64   = 143;
    private static final int REC_BYTE_F_ASCII   = 0;
    private static final int REC_BYTE_V_ASCII   = 64;
    private static final int REC_DATETIME       = 192;

    String getLastError() {
	return null; //lastError;
    }
    
    public String open(String fileName,
                       Object[] colNameList,
                       Object[] colTypeInfoList) throws IOException {

	if (logger.isDebugEnabled()) 
            logger.debug("Enter open()," 
                         + " file name: " + fileName
                         + " colNameList: " + colNameList
                         + " colTypeInfoList: " + colTypeInfoList
                         );

        listOfColName = new ArrayList<String>(colNameList.length);
        for (int i = 0; i < colNameList.length; i++) {
            listOfColName.add((String)colNameList[i]);
        }

        listOfObjectInspector = new ArrayList<ObjectInspector>(colTypeInfoList.length);
        for (int i = 0; i < colTypeInfoList.length; i++) {
            ByteBuffer bb = ByteBuffer.wrap((byte[])colTypeInfoList[i]);
            bb.order(ByteOrder.LITTLE_ENDIAN);

            int type = bb.getInt();
            //            int length  = bb.getInt();
            //            int precision = bb.getInt();
            //            int scale = bb.getInt();
            //            short nullable = bb.getShort();

            //            System.out.println("i = " + i + "type = " + type + ", length = " + length);
            //            if (length > maxLength)
            //                maxLength = length;

            ObjectInspector oi;
            switch (type) {
            case REC_BIN16_SIGNED:
            case REC_BIN16_UNSIGNED:
                oi = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
                break;

            case REC_BIN32_SIGNED:
            case REC_BIN32_UNSIGNED:
                oi = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
                break;

            case REC_BIN64_SIGNED:
                oi = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                break;

            case REC_IEEE_FLOAT32:
                oi = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
                break;

            case REC_IEEE_FLOAT64:
                oi = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                break;

            case REC_BYTE_F_ASCII:
            case REC_BYTE_V_ASCII:
                oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                break;
                
            case REC_DATETIME:
                oi = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                break;

            default:
                oi = PrimitiveObjectInspectorFactory.writableStringObjectInspector;     
                break;
            } // switch
            listOfObjectInspector.add(oi);

        } // for

        sloi = 
            ObjectInspectorFactory.getStandardStructObjectInspector(listOfColName, listOfObjectInspector);

        writer = OrcFile.createWriter(new Path(fileName),
                                      OrcFile.writerOptions(conf)
                                      .inspector(sloi)
                                      .stripeSize(100000)
                                      .bufferSize(10000)
                                      .compress(CompressionKind.ZLIB)
                                      .version(OrcFile.Version.V_0_12));
        
        return null;
    }

    public String close() throws IOException {
	if (logger.isDebugEnabled()) 
            logger.debug("Enter close() " 
                         );
        
        writer.close();

        return null;
    }

    public String insertRows(Object buffer, int bufMaxLen, 
                             int numRows, int bufCurrLen) throws IOException {

	if (logger.isDebugEnabled()) 
            logger.debug("Enter insertRows()," 
                         + " bufMaxLen: " + bufMaxLen
                         + " bufCurrLen: " + bufCurrLen
                         + " numRows: " + numRows
                         );

        ByteBuffer bb = (ByteBuffer)buffer;

        bb.order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < numRows; i++) {
            Object[] colVals = new Object[listOfObjectInspector.size()];

            for (int j = 0; j < listOfObjectInspector.size(); j++) {

                ObjectInspector oi = listOfObjectInspector.get(j);

                short nullVal = bb.getShort();
                int length  = 0;
                if (nullVal == 0)
                    {
                        length = bb.getInt();
                    }

                if (nullVal != 0) {
                    colVals[j] = nullValue; //new NullWritable();
                }
                else if ((nullVal == 0) && (length >= 0)) {
                    byte[] colVal = new byte[length];
                    bb.get(colVal, 0, length);
                    
                    if (oi.getTypeName() == "short") {
                        colVals[j] = new ShortWritable();
                        ((ShortWritable)colVals[j]).set
                            (Short.parseShort(Bytes.toString(colVal, 0, length)));
                    }
                    else if (oi.getTypeName() == "int") {
                        colVals[j] = new IntWritable();
                        ((IntWritable)colVals[j]).set
                            (Integer.parseInt(Bytes.toString(colVal, 0, length)));
                    }
                    else if (oi.getTypeName() == "long") {
                        colVals[j] = new LongWritable();
                        ((LongWritable)colVals[j]).set
                            (Integer.parseInt(Bytes.toString(colVal, 0, length)));
                    }
                    else if (oi.getTypeName() == "float") {
                        colVals[j] = new FloatWritable();
                        ((FloatWritable)colVals[j]).set
                            (Float.parseFloat(Bytes.toString(colVal, 0, length)));
                    }
                    else if (oi.getTypeName() == "double") {
                        colVals[j] = new DoubleWritable();
                        ((DoubleWritable)colVals[j]).set
                            (Double.parseDouble(Bytes.toString(colVal, 0, length)));
                    }
                    else if (oi.getTypeName() == "string") {
                        colVals[j] = new Text();
                        ((Text)colVals[j]).set(Bytes.toString(colVal, 0, length));
                    }
                   else if (oi.getTypeName() == "timestamp") {
                       colVals[j] = new TimestampWritable();
                       Timestamp ts = new Timestamp(2016,05,20,01,01,01,10);
                       ((TimestampWritable)colVals[j]).set(ts);
                       // ((TimestampWritable)colVals[j]).set(Bytes.toString(colVal, 0, length));
                    }
                 }
            }

            writer.addRow(colVals);

        } // for
            
        return null;
    }

    OrcFileWriter() {
	if (logger.isTraceEnabled()) logger.trace("Enter OrcFileWriter()");
        conf = new Configuration();
    }

    public static void main(String[] args) throws IOException,
                                                InterruptedException,
                                                ClassNotFoundException {

  }
}