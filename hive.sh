CREATE EXTERNAL TABLE pravin.orders_simple(order_id INT, dates STRING,
     customer_id INT, status STRING)
 COMMENT 'This is a orders table created from sequence file'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION '/user/cloudera/retail_db_simple/orders/';

CREATE TABLE pravin.de(departments_id INT, name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS SEQUENCEFILE
LOCATION "";

CREATE EXTERNAL TABLE if not exists pravin.de(departments_id INT, name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
LOCATION '/user/cloudera/departments_seq_zip'

CREATE EXTERNAL TABLE de
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION "/user/cloudera/departments_avro"
  TBLPROPERTIES (
    'avro.schema.url'='file:////home/cloudera/departments.avsc')
;

hive -e 'use pravin;show tables' | xargs -I '{}' hive -e 'use pravin;DROP TABLE IF EXISTS {}'

INSERT OVERWRITE TABLE pravin.departments SELECT department_id, department_name from default.departments; 

load data inpath /user/cloudera/departments_seq_zip into table de;