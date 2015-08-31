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

#ifndef TMTXKEY_H_
#define TMTXKEY_H_

#include "dtm/tmtransid.h"


// This class is used to collapse either (nid, sequence number) or (nid, pid) into an int64 
// key value used for maps and dequeues.
class CTmTxKey 
{
private:
    union {
       int64 iv_id;
       struct {
          int32 iv_seqnum;
          int16 iv_node;
	  int16 iv_cluster_id;
       } s;
       struct {
          int32 iv_pid;
          int16 iv_node;
	  int16 iv_cluster_id;
       } p;
    } iv_key;

public:
    CTmTxKey() {set(0,0);}
    //    CTmTxKey(int32 pv_node, int32 pv_seqnum) {set(pv_node, pv_seqnum);}
    CTmTxKey(int16 pv_cluster_id, int32 pv_node, int32 pv_seqnum) {set(pv_cluster_id, (int16) pv_node, pv_seqnum);}
    CTmTxKey(int64 pv_id) {set(pv_id);}
    ~CTmTxKey() {}

    // Get/set methods
    int64 id() {return iv_key.iv_id;}
    void set(int64 pv_id) {iv_key.iv_id = pv_id;}
    void set(int32 pv_node, int32 pv_seqnum)
    {
        iv_key.s.iv_node = pv_node;
        iv_key.s.iv_seqnum = pv_seqnum;
    }
    void set(int16 pv_cluster_id, int16 pv_node, int32 pv_seqnum)
    {
        iv_key.s.iv_cluster_id = pv_cluster_id;
        iv_key.s.iv_node       = pv_node;
        iv_key.s.iv_seqnum     = pv_seqnum;
    }
    int32 node() {
       if (id() == -1)
          return -1;
       else
          return iv_key.s.iv_node;
    }
    int32 seqnum() {
       if (id() == -1)
          return -1;
       else
          return  iv_key.s.iv_seqnum;
    }
    int32 pid() {return iv_key.p.iv_pid;}
    TM_Txid_Internal transid();
};
#endif // TMTXKEY_H_
