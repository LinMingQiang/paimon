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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** CherryPickSnapshotProcedure. */
public class CherryPickSnapshotProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "cherry_pick";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "branch", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "snapshot", type = @DataTypeHint("Integer")),
                    @ArgumentHint(name = "syncOptions", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext, String tableId, String branchName, Integer snapshot, Boolean syncOptions)
            throws Catalog.TableNotExistException {
        Identifier identifier = Identifier.fromString(tableId);
        FileStoreTable mainTable = (FileStoreTable) catalog.getTable(identifier);
        FileStoreTable branchTable = mainTable.switchToBranch(branchName);
        Snapshot cherryPickSnapshot = branchTable.snapshot(snapshot);
        Preconditions.checkArgument(
                cherryPickSnapshot != null
                        && cherryPickSnapshot.commitKind() == Snapshot.CommitKind.APPEND,
                "Cherry-pick only support APPEND commitKind snapshot.");

        Preconditions.checkArgument(
                mainTable.schemaManager().latest().isPresent(), "Main branch has no schema found.");

        ManifestList manifestListReader = branchTable.store().manifestListFactory().create();
        ManifestFile manifestFileReader = branchTable.store().manifestFileFactory().create();

        TableSchema branchSchema =
                branchTable.schemaManager().schema(cherryPickSnapshot.schemaId());

        TableSchema oldSchema = mainTable.schemaManager().latest().get();
        TableSchema updatedSchema = null;
        try {

            Optional<TableSchema> optional =
                    mainTable
                            .schemaManager()
                            .mergeSchema(oldSchema, branchSchema, syncOptions == null || syncOptions, true);
            if (optional.isPresent()) {
                updatedSchema = optional.get();
            }

            List<ManifestEntry> appendTableFiles = new ArrayList<>();
            List<ManifestEntry> appendChangelog = new ArrayList<>();
            List<IndexManifestEntry> appendHashIndexFiles =
                    branchTable
                            .store()
                            .indexManifestFileFactory()
                            .create()
                            .read(cherryPickSnapshot.indexManifest());

            readManifestEntry(
                    cherryPickSnapshot,
                    manifestListReader,
                    manifestFileReader,
                    appendTableFiles,
                    updatedSchema);
            readManifestEntry(
                    cherryPickSnapshot,
                    manifestListReader,
                    manifestFileReader,
                    appendChangelog,
                    updatedSchema);

            FileStoreCommitImpl fileStoreCommit =
                    (FileStoreCommitImpl)
                            mainTable.store().newCommit(cherryPickSnapshot.commitUser(), mainTable);
            fileStoreCommit.commit(
                    appendTableFiles,
                    appendChangelog,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    appendHashIndexFiles,
                    Collections.emptyList(),
                    cherryPickSnapshot.commitIdentifier(),
                    cherryPickSnapshot.watermark(),
                    cherryPickSnapshot.logOffsets(),
                    false);
            fileStoreCommit.close();
        } catch (Throwable e) {
            if (updatedSchema != null) {
                // TODO : 回滚操作.
            }
        }

        return new String[] {"Success"};
    }

    public List<ManifestEntry> updateDataFileMetaSchemaId(
            List<ManifestEntry> manifestEntries, TableSchema updateSchema) {
        return updateSchema == null
                ? manifestEntries
                : manifestEntries.stream()
                        .map(x -> x.copyWithNewFile(x.file().newSchemaId(updateSchema.id())))
                        .collect(Collectors.toList());
    }

    public void readManifestEntry(
            Snapshot cherryPickSnapshot,
            ManifestList manifestListReader,
            ManifestFile manifestFileReader,
            List<ManifestEntry> manifestEntryList,
            TableSchema updateSchema) {
        for (ManifestFileMeta manifestFileMeta :
                manifestListReader.readDeltaManifests(cherryPickSnapshot)) {
            List<ManifestEntry> manifestEntries =
                    manifestFileReader.read(manifestFileMeta.fileName());
            manifestEntryList.addAll(updateDataFileMetaSchemaId(manifestEntries, updateSchema));
        }
    }
}
