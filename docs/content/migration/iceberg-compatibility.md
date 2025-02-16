---
title: "Iceberg Compatibility"
weight: 4
type: docs
aliases:
- /migration/iceberg-compatibility.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg Compatibility

Paimon supports generating Iceberg compatible metadata,
so that Paimon tables can be consumed directly by Iceberg readers.

## Enable Iceberg Compatibility

Set the following table options, so that Paimon tables can generate Iceberg compatible metadata.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>metadata.iceberg.storage</h5></td>
      <td style="word-wrap: break-word;">disabled</td>
      <td>Enum</td>
      <td>
        When set, produce Iceberg metadata after a snapshot is committed, so that Iceberg readers can read Paimon's raw data files.
        <ul>
          <li>disabled: Disable Iceberg compatibility support.</li>
          <li>table-location: Store Iceberg metadata in each table's directory.</li>
          <li>hadoop-catalog: Store Iceberg metadata in a separate directory. This directory can be specified as the warehouse directory of an Iceberg Hadoop catalog.</li>
        </ul>
      </td>
    </tr>
    </tbody>
</table>

For most SQL users, we recommend setting `'metadata.iceberg.storage' = 'hadoop-catalog'`,
so that all tables can be visited as an Iceberg warehouse.
For Iceberg Java API users, you might consider setting `'metadata.iceberg.storage' = 'table-location'`,
so you can visit each table with its table path.

## Example: Query Paimon Append Only Tables with Iceberg Connector

Let's walk through a simple example, where we query Paimon tables with Iceberg connectors in Flink and Spark.
Before trying out this example, make sure that your compute engine already supports Iceberg.
Please refer to Iceberg's document if you haven't set up Iceberg.
* Flink: [Preparation when using Flink SQL Client](https://iceberg.apache.org/docs/latest/flink/#preparation-when-using-flink-sql-client)
* Spark: [Using Iceberg in Spark 3](https://iceberg.apache.org/docs/latest/spark-getting-started/#using-iceberg-in-spark-3)

Let's now create a Paimon append only table with Iceberg compatibility enabled and insert some data.

{{< tabs "create-paimon-append-only-table" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE TABLE paimon_catalog.`default`.cities (
    country STRING,
    name STRING
) WITH (
    'metadata.iceberg.storage' = 'hadoop-catalog'
);

INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'new york'), ('germany', 'berlin'), ('usa', 'chicago'), ('germany', 'hamburg');
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
Start `spark-sql` with the following command line.

```bash
spark-sql --jars <path-to-paimon-jar> \
    --conf spark.sql.catalog.paimon_catalog=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon_catalog.warehouse=/tmp/sparkware \
    --packages org.apache.iceberg:iceberg-spark-runtime-<iceberg-version> \
    --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg_catalog.type=hadoop \
    --conf spark.sql.catalog.iceberg_catalog.warehouse=<path-to-warehouse>/iceberg \
    --conf spark.sql.catalog.iceberg_catalog.cache-enabled=false \ # disable iceberg catalog caching to quickly see the result
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Run the following Spark SQL to create Paimon table and insert data.

```sql
CREATE TABLE paimon_catalog.`default`.cities (
    country STRING,
    name STRING
) TBLPROPERTIES (
    'metadata.iceberg.storage' = 'hadoop-catalog'
);

INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'new york'), ('germany', 'berlin'), ('usa', 'chicago'), ('germany', 'hamburg');
```
{{< /tab >}}

{{< /tabs >}}

Now let's query this Paimon table with Iceberg connector.

{{< tabs "query-paimon-append-only-table" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE TABLE cities (
    country STRING,
    name STRING
) WITH (
    'connector' = 'iceberg',
    'catalog-type' = 'hadoop',
    'catalog-name' = 'test',
    'catalog-database' = 'default',
    'warehouse' = '<path-to-warehouse>/iceberg'
);

SELECT * FROM cities WHERE country = 'germany';
/*
+----+--------------------------------+--------------------------------+
| op |                        country |                           name |
+----+--------------------------------+--------------------------------+
| +I |                        germany |                         berlin |
| +I |                        germany |                        hamburg |
+----+--------------------------------+--------------------------------+
*/
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
```sql
SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany';
/*
germany berlin
germany hamburg
*/
```
{{< /tab >}}

{{< /tabs >}}

Let's insert more data and query again.

{{< tabs "query-paimon-append-only-table-again" >}}

{{< tab "Flink SQL" >}}
```sql
INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'houston'), ('germany', 'munich');

SELECT * FROM cities WHERE country = 'germany';
/*
+----+--------------------------------+--------------------------------+
| op |                        country |                           name |
+----+--------------------------------+--------------------------------+
| +I |                        germany |                         munich |
| +I |                        germany |                         berlin |
| +I |                        germany |                        hamburg |
+----+--------------------------------+--------------------------------+
*/
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
```sql
INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'houston'), ('germany', 'munich');

SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany';
/*
germany munich
germany berlin
germany hamburg
*/
```
{{< /tab >}}

{{< /tabs >}}

## Example: Query Paimon Primary Key Tables with Iceberg Connector

{{< tabs "paimon-primary-key-table" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE TABLE paimon_catalog.`default`.orders (
    order_id BIGINT,
    status STRING,
    payment DOUBLE,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'metadata.iceberg.storage' = 'hadoop-catalog',
    'compaction.optimization-interval' = '1ms' -- ATTENTION: this option is only for testing, see "timeliness" section below for more information
);

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'SUBMITTED', CAST(NULL AS DOUBLE)), (2, 'COMPLETED', 200.0), (3, 'SUBMITTED', CAST(NULL AS DOUBLE));

CREATE TABLE orders (
    order_id BIGINT,
    status STRING,
    payment DOUBLE
) WITH (
    'connector' = 'iceberg',
    'catalog-type' = 'hadoop',
    'catalog-name' = 'test',
    'catalog-database' = 'default',
    'warehouse' = '<path-to-warehouse>/iceberg'
);

SELECT * FROM orders WHERE status = 'COMPLETED';
/*
+----+----------------------+--------------------------------+--------------------------------+
| op |             order_id |                         status |                        payment |
+----+----------------------+--------------------------------+--------------------------------+
| +I |                    2 |                      COMPLETED |                          200.0 |
+----+----------------------+--------------------------------+--------------------------------+
*/

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'COMPLETED', 100.0);

SELECT * FROM orders WHERE status = 'COMPLETED';
/*
+----+----------------------+--------------------------------+--------------------------------+
| op |             order_id |                         status |                        payment |
+----+----------------------+--------------------------------+--------------------------------+
| +I |                    1 |                      COMPLETED |                          100.0 |
| +I |                    2 |                      COMPLETED |                          200.0 |
+----+----------------------+--------------------------------+--------------------------------+
*/
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
Start `spark-sql` with the following command line.

```bash
spark-sql --jars <path-to-paimon-jar> \
    --conf spark.sql.catalog.paimon_catalog=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon_catalog.warehouse=/tmp/sparkware \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
    --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg_catalog.type=hadoop \
    --conf spark.sql.catalog.iceberg_catalog.warehouse=<path-to-warehouse>/iceberg \
    --conf spark.sql.catalog.iceberg_catalog.cache-enabled=false \ # disable iceberg catalog caching to quickly see the result
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Run the following Spark SQL to create Paimon table, insert/update data, and query with Iceberg catalog. 

```sql
CREATE TABLE paimon_catalog.`default`.orders (
    order_id BIGINT,
    status STRING,
    payment DOUBLE
) TBLPROPERTIES (
    'primary-key' = 'order_id',
    'metadata.iceberg.storage' = 'hadoop-catalog',
    'compaction.optimization-interval' = '1ms' -- ATTENTION: this option is only for testing, see "timeliness" section below for more information
);

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'SUBMITTED', CAST(NULL AS DOUBLE)), (2, 'COMPLETED', 200.0), (3, 'SUBMITTED', CAST(NULL AS DOUBLE));

SELECT * FROM iceberg_catalog.`default`.orders WHERE status = 'COMPLETED';
/*
2       COMPLETED       200.0
*/

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'COMPLETED', 100.0);

SELECT * FROM iceberg_catalog.`default`.orders WHERE status = 'COMPLETED';
/*
2       COMPLETED       200.0
1       COMPLETED       100.0
*/
```
{{< /tab >}}

{{< /tabs >}}

### Timeliness

Paimon primary key tables organize data files as [LSM trees]({{< ref "learn-paimon/understand-files/#understand-lsm-for-primary-table" >}}), so data files must be merged in memory before querying.
However, Iceberg readers are not able to merge data files, so they can only query data files on the highest level of LSM trees.
Data files on the highest level are produced by the full compaction process.
So **to conclude, for primary key tables, Iceberg readers can only query data after full compaction**.

By default, there is no guarantee on how frequently Paimon will perform full compaction.
You can configure the following table option, so that Paimon is forced to perform full compaction after several commits.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>compaction.optimization-interval</h5></td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>Full compaction will be constantly triggered per time interval. First compaction after the job starts will always be full compaction.</td>
    </tr>
    <tr>
      <td><h5>full-compaction.delta-commits</h5></td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Full compaction will be constantly triggered after delta commits. Only implemented in Flink.</td>
    </tr>
    </tbody>
</table>

Note that full compaction is a resource-consuming process, so the value of this table option should not be too small.
We recommend full compaction to be performed once or twice per hour.

## Other Related Table Options

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>metadata.iceberg.compaction.min.file-num</h5></td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>Minimum number of Iceberg metadata files to trigger metadata compaction.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.compaction.max.file-num</h5></td>
      <td style="word-wrap: break-word;">50</td>
      <td>Integer</td>
      <td>If number of small Iceberg metadata files exceeds this limit, always trigger metadata compaction regardless of their total size.</td>
    </tr>
    </tbody>
</table>
