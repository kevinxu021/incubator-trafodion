DROP TABLE MYORC;
CREATE TABLE MYORC
(
  ID                  int
, COL1                int
, COL2                int
, COL3                date
)
PARTITIONED BY (PCOL STRING) 
CLUSTERED BY (COL1) 
INTO 8 BUCKETS STORED AS ORC;

SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE MYORC PARTITION (PCOL='one') 
VALUES 
(0,39478,8147,'2008-07-17'),
(1,21944,8327,'2005-05-12'),
(2,32730,9999,'2000-11-05'),
(3,19653,5727,'2005-06-24'),
(4,67794,6012,'2008-07-01');

INSERT OVERWRITE TABLE MYORC PARTITION (PCOL='two') 
VALUES 
(5,93265,5823,'2012-06-26'),
(6,28219,909,'2009-04-26'),
(7,23967,8290,'2006-02-21'),
(8,24265,8663,'2006-10-06'),
(9,70273,3363,'2001-03-17');

