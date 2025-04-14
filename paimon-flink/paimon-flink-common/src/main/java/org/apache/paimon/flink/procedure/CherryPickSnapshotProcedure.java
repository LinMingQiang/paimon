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
                @ArgumentHint(
                        name = "syncOptions",
                        type = @DataTypeHint("BOOLEAN"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String branchName,
            Integer snapshot,
            Boolean syncOptions)
            throws Catalog.TableNotExistException {
        Identifier identifier = Identifier.fromString(tableId);
        FileStoreTable mainTable = (FileStoreTable) catalog.getTable(identifier);
        Snapshot updatedSnapshot = mainTable.cherryPick(branchName, snapshot);
//        FileStoreTable branchTable = mainTable.switchToBranch(branchName);
//        Snapshot cherryPickSnapshot = branchTable.snapshot(snapshot);
//        Preconditions.checkArgument(
//                cherryPickSnapshot != null
//                        && cherryPickSnapshot.commitKind() == Snapshot.CommitKind.APPEND,
//                "Cherry-pick only support APPEND commitKind snapshot.");
//
//        Optional<Snapshot> oldSnapshot = mainTable.latestSnapshot();
//        TableSchema oldSchema = mainTable.schemaManager().latest().get();
//
//        TableSchema branchSchema =
//                branchTable.schemaManager().schema(cherryPickSnapshot.schemaId());
//        TableSchema updatedSchema = null;
//        Snapshot updatedSnapshot;
//        try {
//
//            Optional<TableSchema> optional =
//                    mainTable
//                            .schemaManager()
//                            .mergeSchema(
//                                    oldSchema,
//                                    branchSchema,
//                                    syncOptions == null || syncOptions,
//                                    true);
//            if (optional.isPresent()) {
//                updatedSchema = optional.get();
//            }
//
//            ManifestList manifestListReader = branchTable.store().manifestListFactory().create();
//            ManifestFile manifestFileReader = branchTable.store().manifestFileFactory().create();
//
//            List<ManifestEntry> appendTableFiles = new ArrayList<>();
//            List<ManifestEntry> appendChangelog = new ArrayList<>();
//            List<IndexManifestEntry> appendHashIndexFiles =
//                    branchTable
//                            .store()
//                            .indexManifestFileFactory()
//                            .create()
//                            .read(cherryPickSnapshot.indexManifest());
//
//            // 读取 append 文件.
//            readAndUpdateManifestEntry(
//                    manifestFileReader,
//                    manifestListReader.readDeltaManifests(cherryPickSnapshot),
//                    appendTableFiles,
//                    updatedSchema);
//
//            // 读取 change log
//            readAndUpdateManifestEntry(
//                    manifestFileReader,
//                    manifestListReader.readChangelogManifests(cherryPickSnapshot),
//                    appendChangelog,
//                    updatedSchema);
//
//            FileStoreCommitImpl fileStoreCommit =
//                    (FileStoreCommitImpl)
//                            mainTable.store().newCommit(cherryPickSnapshot.commitUser(), mainTable);
//            fileStoreCommit.commit(
//                    appendTableFiles,
//                    appendChangelog,
//                    Collections.emptyList(),
//                    Collections.emptyList(),
//                    appendHashIndexFiles,
//                    Collections.emptyList(),
//                    cherryPickSnapshot.commitIdentifier(),
//                    cherryPickSnapshot.watermark(),
//                    cherryPickSnapshot.logOffsets(),
//                    false);
//            fileStoreCommit.close();
//            updatedSnapshot = mainTable.store().snapshotManager().latestSnapshot();
//        } catch (Throwable e) {
//            if (updatedSchema != null) {
//                Long latestSnpId = mainTable.store().snapshotManager().latestSnapshotId();
//                if (latestSnpId != null) {
//                    if (!oldSnapshot.isPresent() || oldSnapshot.get().id() < latestSnpId) {
//                        mainTable
//                                .fileIO()
//                                .deleteQuietly(
//                                        mainTable.schemaManager().toSchemaPath(updatedSchema.id()));
//                    }
//                }
//            }
//            throw e;
//        }

        return new String[] {
            updatedSnapshot == null
                    ? "Cherry-pick failed"
                    : "Cherry-pick to snapshotID : " + updatedSnapshot.id()
        };
    }

    public void readAndUpdateManifestEntry(
            ManifestFile manifestFileReader,
            List<ManifestFileMeta> manifestFileMetas,
            List<ManifestEntry> manifestEntryList,
            TableSchema updateSchema) {
        for (ManifestFileMeta manifestFileMeta : manifestFileMetas) {
            List<ManifestEntry> manifestEntries =
                    manifestFileReader.read(manifestFileMeta.fileName());
            // update schemaId.
            if (updateSchema != null) {
                manifestEntries =
                        manifestEntries.stream()
                                .map(
                                        x ->
                                                x.copyWithNewFile(
                                                        x.file().newSchemaId(updateSchema.id())))
                                .collect(Collectors.toList());
            }
            manifestEntryList.addAll(manifestEntries);
        }
    }
}
