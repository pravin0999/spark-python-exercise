sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root --password cloudera \
--table orders -m '4' --as-avrodatafile --warehouse-dir . \
--split-by order_id --fields-terminated-by '|' \
--lines-terminated-by '\n'

sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root --password cloudera --as-sequencefile \
--target-dir /user/cloudera/department_seq \
-e 'select department_id, department_name from departments where $CONDITIONS' \
--boundary-query 'SELECT MIN(department_id), MAX(department_id) FROM departments AS t1' \
--where "department_id > 4" -z --fields-terminated-by "|" \
--lines-terminated-by '\n' --split-by 'department_id'

sqoop export --connect jdbc:mysql://quickstart.cloudera:3306/pravin \
--username root --password cloudera \
--export-dir /user/cloudera/department_seq \
-m '4' --table departments --input-fields-terminated-by "|" \
--input-lines-terminated-by '\n'


sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root --password cloudera --as-sequencefile \
--target-dir /user/cloudera/department_seq \
-table departments \
-z 

sqoop export --connect jdbc:mysql://quickstart.cloudera:3306/pravin \
--username root --password cloudera \
--export-dir /user/cloudera/department_seq \
--table departments 

sqoop import-all-tables --connect jdbc:mysql://\
quickstart.cloudera:3306/retail_db \
--username root --password cloudera \
-m=8 -z --hive-database='pravin' \
--hive-import --hive-overwrite --create-hive-table \
--outdir='java_files' 

sqoop import-all-tables --connect jdbc:mysql://\
quickstart.cloudera:3306/retail_db \
--username root --password cloudera \
-m=8 -z --as-sequencefile \
--warehouse-dir="/user/cloudera/retail_db_seq"

sqoop import \
  --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
  --username=retail_dba \
  --password=cloudera \
  --query="select * from orders join order_items on orders.order_id = order_items.order_item_order_id where \$CONDITIONS" \
  --target-dir /user/cloudera/order_join \
  --split-by order_id \
  --num-mappers 4