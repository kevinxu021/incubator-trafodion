#######################################################################
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#######################################################################

INCLUDE_DIRS += sqludr
# Define the correct compilation and linking flags depending on whether
# this is a debug or release build.
ifeq ($(FLAVOR),debug)
   # for coverage checking support
   ifeq ($(SQ_COVERAGE_OPTIMIZER),1)
      CXXFLAGS += --coverage
   endif
endif

#GUI Debugger
# Like in sqlcomp, enable this unconditionally, to be able to display in release mode
# if the necessary DLL is present
CXXFLAGS += -DNA_DEBUG_GUI

SYS_LIBS += -L$(PROTOBUFS_LIB) -lprotobuf  \
            -L$(SNAPPY_LIB) -lsnappy

SRCPATH += orc/src

CPPSRC := \
    ByteRLE.cpp         \
    ColumnPrinter.cpp   \
    ColumnReader.cpp   \
    Compression.cpp    \
    Exceptions.cpp     \
    Int128.cpp        \
    MemoryPool.cpp    \
    OrcFile.cpp       \
    Reader.cpp     \
    RLEv1.cpp   \
    RLEv2.cpp   \
    RLE.cpp     \
    Timezone.cpp \
    TypeImpl.cpp \
    Vector.cpp \
    orc-proto-wrapper.cpp \
    HdfsOrcFile.cpp 
