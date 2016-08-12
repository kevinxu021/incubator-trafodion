create table LINEITEM
  (            
     L_ORDERKEY                   LARGEINT NO DEFAULT NOT NULL NOT DROPPABLE     
   , L_PARTKEY                    INT NO DEFAULT NOT NULL NOT DROPPABLE
   , L_SUPPKEY                    INT NO DEFAULT NOT NULL NOT DROPPABLE
   , L_LINENUMBER                 INT NO DEFAULT NOT NULL NOT DROPPABLE    
   , L_QUANTITY                   DECIMAL(15,2) NO DEFAULT NOT NULL NOT DROPPABLE 
   , L_EXTENDEDPRICE              DECIMAL(15,2) NO DEFAULT NOT NULL NOT DROPPABLE
   , L_DISCOUNT                   DECIMAL(15,2) NO DEFAULT NOT NULL NOT DROPPABLE
   , L_TAX                        DECIMAL(15,2) NO DEFAULT NOT NULL NOT DROPPABLE
   , L_RETURNFLAG                 CHAR(1) NO DEFAULT NOT NULL NOT DROPPABLE
   , L_LINESTATUS                 CHAR(1) NO DEFAULT NOT NULL NOT DROPPABLE
   , L_SHIPDATE                   DATE NO DEFAULT NOT NULL NOT DROPPABLE                                
   , L_COMMITDATE                 DATE NO DEFAULT NOT NULL NOT DROPPABLE                                
   , L_RECEIPTDATE                DATE NO DEFAULT NOT NULL NOT DROPPABLE                                
   , L_SHIPINSTRUCT               CHAR(25) NO DEFAULT NOT NULL NOT DROPPABLE                            
   , L_SHIPMODE                   CHAR(10) NO DEFAULT NOT NULL NOT DROPPABLE                            
   , L_COMMENT                    VARCHAR(44) NO DEFAULT NOT NULL NOT DROPPABLE     
  , PRIMARY KEY (L_ORDERKEY ASC, L_LINENUMBER ASC) NOT DROPPABLE
  )
  SALT USING 48 PARTITIONS
  HBASE_OPTIONS
  (
    DATA_BLOCK_ENCODING = 'FAST_DIFF',
    COMPRESSION = 'SNAPPY',
    MEMSTORE_FLUSH_SIZE = '1073741824'
  )
; 

