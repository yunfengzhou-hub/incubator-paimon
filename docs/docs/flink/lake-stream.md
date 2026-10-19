---
title: "Lake-Stream Mode"
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

# Lake-Stream Mode

The Flink connector routes the reads and writes of a lake-stream table through the streaming store
you name on the catalog. [Lake-Stream Mode](../concepts/lake-stream) explains what the mode does to
a table. This page covers the Flink catalog configuration and the Flink SQL surface.

## Enable Lake-Stream Mode

Enabling a table takes four steps, and the store builds its own side of the table during them.

1. Stop every pipeline that writes to the table. Writes that land while the store initializes can
   leave the store and Paimon holding different data.
2. Run the store's enablement procedure against the table. For Fluss this is
   `sys.enable_fluss_on_lake_table`, and it runs in a Fluss catalog rather than a Paimon one. It
   creates the matching Fluss table, loads the existing Paimon data for a primary-key table, and
   then sets `lakestream.enabled` to `true` on the Paimon table. Check the Fluss documentation for
   the current syntax and prerequisites.
3. Create the Paimon catalog with `lakestream.store` and the store's connection options.
4. Restart the write pipelines without state, so that Flink plans them against the store sink.

```sql
CREATE CATALOG `paimon_catalog` WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///path/to/warehouse',
    'lakestream.store' = 'fluss',
    'fluss.bootstrap.servers' = 'fluss-server:9123'
);
```

`lakestream.store` names the store implementation. Prefix every store connection option with that
identifier, and Paimon removes the prefix before it passes the options on, so
`fluss.bootstrap.servers` reaches Fluss as `bootstrap.servers`. See
[CatalogOptions](../maintenance/configurations#catalogoptions) for the catalog reference.

Your write statements do not change. Flink reads `lakestream.enabled` from the table when it plans
the statement and routes the write accordingly.

```sql
INSERT INTO orders SELECT * FROM upstream_orders;
```

A catalog created without `lakestream.store` leaves the table behaving as an ordinary Paimon table.
`orders$rt` fails as an unknown table, and writes go to Paimon instead of the store.

A Flink CDC pipeline does not plan its writes through the SQL sink, so it writes straight to Paimon
and bypasses the store the same way `INSERT OVERWRITE` does. Apply the same precaution: confirm the
data it writes has already expired from the store. See
[Write to a Lake-Stream Table](../concepts/lake-stream#write-to-a-lake-stream-table).

## Query the Real-Time Alias

Write the alias in backticks:

```sql
-- Reads only data that Paimon has committed.
SELECT * FROM orders;

-- Combines committed Paimon data with the store's un-tiered records.
SELECT * FROM `orders$rt`;
```

Paimon connects to the store on the first lake-stream read or write. An unknown store identifier or
a connection option the store requires fails at that point with a configuration error.
