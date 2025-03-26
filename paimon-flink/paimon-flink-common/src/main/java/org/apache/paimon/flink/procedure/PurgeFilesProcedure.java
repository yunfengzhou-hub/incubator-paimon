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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A procedure to purge files for a table. Usage:
 *
 * <pre><code>
 *  -- rollback to the snapshot which earlier or equal than watermark.
 *  CALL sys.purge_files(`table` => 'tableId')
 * </code></pre>
 */
public class PurgeFilesProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "purge_files";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dry_run", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext, String tableId, @Nullable Boolean nullableDryRun)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileIO fileIO = fileStoreTable.fileIO();
        Path tablePath = fileStoreTable.snapshotManager().tablePath();
        List<Path> filePaths;
        boolean dryRun = nullableDryRun != null && nullableDryRun;
        try {
            filePaths =
                    Arrays.stream(fileIO.listStatus(tablePath))
                            .filter(f -> !f.getPath().getName().contains("schema"))
                            .map(FileStatus::getPath)
                            .collect(Collectors.toList());
            if (!dryRun) {
                filePaths.forEach(
                        filePath -> {
                            try {
                                fileIO.delete(filePath, true);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (dryRun) {
            return new String[] {
                String.format(
                        "Successfully analyzed files to purge with table: %s. Files to purge: %s",
                        fileStoreTable.name(), filePaths)
            };
        } else {
            return new String[] {
                String.format(
                        "Successfully purged files with table: %s. Purged files: %s",
                        fileStoreTable.name(), filePaths)
            };
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
