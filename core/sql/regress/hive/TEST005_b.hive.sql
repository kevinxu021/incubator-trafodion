-- @@@ START COPYRIGHT @@@
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- @@@ END COPYRIGHT @@@

insert overwrite table customer_ddl
select 
    c_customer_sk,
    c_customer_id,
    c_current_cdemo_sk,
    c_current_hdemo_sk,
    c_current_addr_sk,
    c_first_shipto_date_sk,
    c_first_sales_date_sk,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date
from customer 
where c_customer_sk < 20000;

set hive.enforce.bucketing = true;  
insert overwrite table customer_bp partition (c_preferred_cust_flag='N') 
select 
    c_customer_sk,
    c_customer_id,
    c_current_cdemo_sk,
    c_current_hdemo_sk,
    c_current_addr_sk,
    c_first_shipto_date_sk,
    c_first_sales_date_sk,
    c_salutation,
    c_first_name,
    c_last_name,
    --c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date
from customer_ddl
where c_preferred_cust_flag='N'
      and c_customer_sk < 20000;

drop table newtable;
create table newtable(a string);

drop schema if exists hiveregr5;
create schema hiveregr5;

create table hiveregr5.newtable2(a string);

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table hivepi partition (p1) select id, col2, p1 from hivenonp;
insert into table hiveps partition (p2) select id, col2, p2 from hivenonp;
insert into table hivepis partition (p1, p2) select id, col2, p1, p2 from hivenonp;
insert into table hivepts partition (p1t, p2) select id, col2, p1t, p2 from hivenonp;
insert into table hivepio partition (p1) select id, col2, p1 from hivenonp;
insert into table hivepdo partition (p1d) select id, col2, p1d from hivenonp;
insert into table hivepiso partition (p1, p2) select id, col2, p1, p2 from hivenonp;
