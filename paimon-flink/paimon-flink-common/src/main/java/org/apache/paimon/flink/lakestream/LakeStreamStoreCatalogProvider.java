/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.lakestream;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.DynamicTableFactory;

/**
 * Internal plumbing for lake-stream mode (PIP-39), NOT an external SPI: only {@link
 * LakeStreamFlinkCatalog} implements it, and nothing outside Paimon is expected to.
 *
 * <p>{@link LakeStreamFlinkCatalog} wraps a {@link org.apache.paimon.flink.FlinkCatalog} by
 * composition, so it is not itself a {@code FlinkCatalog} and cannot be handed to {@link
 * org.apache.paimon.flink.AbstractFlinkTableFactory} through its {@code FlinkCatalog} constructor
 * argument. This narrow accessor lets the table factory reach the lake-stream store catalog that
 * the wrapper discovers, caches, opens and closes. Store discovery itself still goes through
 * Flink's {@link org.apache.flink.table.factories.CatalogFactory} SPI.
 */
public interface LakeStreamStoreCatalogProvider {

    /**
     * Returns the cached lake-stream store catalog, lazily discovering, creating and opening it
     * from the {@code lakestream.store} catalog option plus the catalog options prefixed with that
     * identifier and a dot, with the prefix removed. The returned catalog is cached and closed by
     * the wrapper.
     */
    Catalog getOrCreateLakeStreamStoreCatalog(DynamicTableFactory.Context context);
}
