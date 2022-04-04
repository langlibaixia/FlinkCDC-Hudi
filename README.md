# demo_10_hudi

## 1. 概述
        
    FlinkCDC实时数据入湖是当下最具代表性的数据处理架构，本案例实时捕获mysql表变更数据，将products、orders、shipments表关联结果实时入湖，入湖方式读多写少选择MOR,写多读少选择COW。版本为Flink1.13.2,Hudi版本为0.10，hadoop版本为3.2.2，hive版本为3.1.2。主要满足以下场景：
      * 代替传统架构中ODS；
      * 上游实时数据动态捕获；
      * 解决流存储，实现流计算和存储闭环；
      * 向数据应用形成湖仓一体。

    
## 2. 准备
    hudi-flink-bundle_2.12-0.10.0-SNAPSHOT.jar、flink-shaded-hadoop-2-uber-2.8.3-10.0.jar拷贝到 $FLINK_HOME/lib目录下
    hudi-hadoop-mr-bundle-0.9.0-SNAPSHOT.jar拷贝到 $HIVE_HOME/auxlib/目录下
    **启动Flink:**
       start-cluster.sh
    ** 启动hive:**
       hive --service metastore &
       hive --service hiveserver2 &
## 3. Mysql建表
```sql
-- MySQL
CREATE DATABASE appdb;
USE appdb;
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512)
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default,"scooter","Small 2-wheel scooter"),
       (default,"car battery","12V car battery"),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3"),
       (default,"hammer","12oz carpenter's hammer"),
       (default,"hammer","14oz carpenter's hammer"),
       (default,"hammer","16oz carpenter's hammer"),
       (default,"rocks","box of assorted rocks"),
       (default,"jacket","water resistent black wind breaker"),
       (default,"spare tire","24 inch spare tire");

CREATE TABLE orders (
  order_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_date DATETIME NOT NULL,
  customer_name VARCHAR(255) NOT NULL,
  price DECIMAL(10, 5) NOT NULL,
  product_id INTEGER NOT NULL,
  order_status BOOLEAN NOT NULL -- Whether order has been placed
) AUTO_INCREMENT = 10001;

INSERT INTO orders
VALUES (default, '2020-07-30 10:08:22', 'Jark', 50.50, 102, false),
       (default, '2020-07-30 10:11:09', 'Sally', 15.00, 105, false),
       (default, '2020-07-30 12:00:30', 'Edward', 25.25, 106, false);
  

CREATE TABLE shipments (
  shipment_id  INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_id INTEGER NOT NULL,
  origin VARCHAR(255) NOT NULL,
  destination VARCHAR(255) NOT NULL,
  is_arrived BOOLEAN NOT NULL
) AUTO_INCREMENT = 10001;

INSERT INTO shipments
VALUES (default,10001,'Beijing','Shanghai',false),
       (default,10002,'Hangzhou','Shanghai',false),
       (default,10003,'Shanghai','Hangzhou',false);   

 ```
## 4. Flink注册表
[CDC配置参数详情](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#supported-databases)
[Hudi配置参数详情](https://hudi.apache.org/docs/configurations#FLINK_SQL)
 ```sql
--First, enable checkpoints every 3 seconds
-- Flink SQL                   
Flink SQL> SET execution.checkpointing.interval = 3s;

--Then, create tables that capture the change data from the corresponding database tables.

-- Flink SQL
Flink SQL> CREATE TABLE products (
    id INT,
    name STRING,
    description STRING,
    PRIMARY KEY (id) NOT ENFORCED
  ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.1.161',
    'port' = '3306',
    'username' = 'root',
    'password' = '@Rq834009465',
    'database-name' = 'appdb',
    'table-name' = 'products'
  );

Flink SQL> CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.1.161',
    'port' = '3306',
    'username' = 'root',
    'password' = '@Rq834009465',
    'database-name' = 'appdb',
    'table-name' = 'orders'
 );
 
Flink SQL> CREATE TABLE shipments (
   shipment_id INT,
   order_id INT,
   origin STRING,
   destination STRING,
   is_arrived BOOLEAN,
   PRIMARY KEY (shipment_id) NOT ENFORCED
 ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.1.161',
    'port' = '3306',
    'username' = 'root',
    'password' = '@Rq834009465',
    'database-name' = 'appdb',
    'table-name' = 'shipments'
 );

--Finally, create enriched_orders table that is used to load data to the Hudi

-- Flink SQL COW方式入湖
Flink SQL> CREATE TABLE enriched_orders1 (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   product_name STRING,
   product_description STRING,
   shipment_id INT,
   origin STRING,
   destination STRING,
   is_arrived BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) PARTITIONED BY (`origin`)
with(
  'connector'='hudi',
  'path' ='hdfs://cluster/hudi/enriched_orders1',
  'table.type'='COPY_ON_WRITE',        -- MERGE_ON_READ方式在没生成 parquet 文件前，hive不会有输出
  'hive_sync.enable'='true',           -- required，开启hive同步功能
  'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc
  'hive_sync.metastore.uris' = 'thrift://192.168.1.161:9083', -- required, metastore的端口
  'hive_sync.table'='enriched_orders1',                          -- required, hive 新建的表名
  'hive_sync.db'='zhruiqi' ,                       -- required, hive 新建的数据库名
  'compaction.async.enabled'='true',
  'compaction.trigger.strategy'='num_commits',
  'compaction.delta_commits'='3',
  'write.tasks' = '4',
  'compaction.max_memory'='1024',
  'execution.checkpointing.interval'='3000',
  'hoodie.clustering.async.enabled' = 'true',
  'hoodie.clustering.async.max.commits' = '3',
'hoodie.clustering.preserve.commit.metadata'='true',
  'hive_sync.file_format' ='PARQUET',
  'write.insert.cluster'='true',
  'hive_sync.username'='', --required HMS 用户名
  'hive_sync.password'=''
) ;
 
-- Flink SQL MOR方式入湖
Flink SQL> CREATE TABLE enriched_orders2 (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   product_name STRING,
   product_description STRING,
   shipment_id INT,
   origin STRING,
   destination STRING,
   is_arrived BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 )
PARTITIONED BY (`origin`)
with(
  'connector'='hudi',
  'path' ='hdfs://cluster/hudi/enriched_orders2',
  'table.type'='MERGE_ON_READ',        -- MERGE_ON_READ方式在没生成 parquet 文件前，hive不会有输出
  'hive_sync.enable'='true',           -- required，开启hive同步功能
  'hive_sync.mode' = 'hms',            -- required, 将hive sync mode设置为hms, 默认jdbc
  'read.streaming.enabled' = 'true',  
  --'read.streaming.start-commit' = '20210901151206' ,
  'read.streaming.check-interval' = '2',
  'hive_sync.metastore.uris' = 'thrift://192.168.1.161:9083' ,
  'hive_sync.table'='enriched_orders2',           -- required, hive 新建的表名
  'hive_sync.db'='zhruiqi',                       -- required, hive 新建的数据库名
  'write.tasks'='1',
  'compaction.tasks'='1'
) ; 
 ```

## 5. Flink执行sql
```sql
--Use Flink SQL to join the order table with the products and shipments table to enrich orders and write to the Hudi.
-- Flink SQL
Flink SQL> INSERT INTO enriched_orders1
 SELECT o.order_id,
        o.order_date,
        o.customer_name,
        o.price,
        o.product_id,
        o.order_status,
        p.name, 
        p.description, 
        s.shipment_id, 
        s.origin, 
        s.destination, 
        s.is_arrived
 FROM orders AS o
 LEFT JOIN products AS p 
   ON o.product_id = p.id
 LEFT JOIN shipments AS s 
   ON o.order_id = s.order_id ;

Flink SQL> INSERT INTO enriched_orders2
 SELECT o.order_id,
        o.order_date,
        o.customer_name,
        o.price,
        o.product_id,
        o.order_status,
        p.name, 
        p.description, 
        s.shipment_id, 
        s.origin, 
        s.destination, 
        s.is_arrived
 FROM orders AS o
 LEFT JOIN products AS p 
   ON o.product_id = p.id
 LEFT JOIN shipments AS s 
   ON o.order_id = s.order_id ;
 ```
## 6. 测试
```sql
--Next, do some change in the databases, and then the enriched orders shown in Kibana will be updated after each step in real time.
--Insert a new order in MySQL
INSERT INTO orders
VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);

--Insert a shipment in MySQL
INSERT INTO shipments
VALUES (default,10004,'Shanghai','Beijing',false);

--Update the order status in MySQL
UPDATE orders SET order_status = true WHERE order_id = 10004;

--Update the shipment status in MySQL
UPDATE shipments SET is_arrived = true WHERE shipment_id = 10004;

--Delete the order in MySQL
DELETE FROM orders WHERE order_id = 10004;
```
## 4. Hive 查询
    使用 beeline 查询时需要手动设置:
    set hive.input.format = org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;

