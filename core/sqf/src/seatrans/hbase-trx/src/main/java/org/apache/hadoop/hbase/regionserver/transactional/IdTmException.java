/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/

package org.apache.hadoop.hbase.regionserver.transactional;

/**
 * IdTmException
 *
 * exception
 */
public class IdTmException extends Exception {
   /**
    * constructor
    *
    * @param message message
    */
   public IdTmException(String message) {
       super(message);
    }
 /**
 *    * @param arg0 cause
 *       */
  public IdTmException(Throwable arg0) {
    super(arg0);
  }

 /**
 *    * @param arg0 cause
 *       */
  public IdTmException(String message, Throwable arg0) {
    super(message, arg0);
  }

}

