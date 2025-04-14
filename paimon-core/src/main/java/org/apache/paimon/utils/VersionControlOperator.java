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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Version control operator. */
public class VersionControlOperator {
    protected final FileStoreTable masterTable;
    protected boolean overwriteOptions;
    protected final CatalogEnvironment catalogEnvironment;

    public VersionControlOperator(
            FileStoreTable masterTable, CatalogEnvironment catalogEnvironment) {
        this.catalogEnvironment = catalogEnvironment;
        this.masterTable = masterTable;
    }

    /** Cherry-pick snapshot from branch to current branch. */
    public Snapshot cherryPick(String fromBranch, long snapshotId) {
        FileStoreTable branchTable = masterTable.switchToBranch(fromBranch);
        Snapshot cherryPickSnapshot = branchTable.snapshot(snapshotId);
        Preconditions.checkArgument(
                cherryPickSnapshot != null
                        && cherryPickSnapshot.commitKind() == Snapshot.CommitKind.APPEND,
                "Cherry-pick only support APPEND commitKind snapshot.");

        Optional<Snapshot> oldSnapshot = masterTable.latestSnapshot();
        TableSchema oldSchema = masterTable.schemaManager().latest().get();

        TableSchema branchSchema =
                branchTable.schemaManager().schema(cherryPickSnapshot.schemaId());
        TableSchema updatedSchema = null;
        Snapshot updatedSnapshot;
        try {

            Optional<TableSchema> optional =
                    masterTable
                            .schemaManager()
                            .mergeSchema(oldSchema, branchSchema, overwriteOptions, true);
            if (optional.isPresent()) {
                updatedSchema = optional.get();
            }

            ManifestList manifestListReader = branchTable.store().manifestListFactory().create();
            ManifestFile manifestFileReader = branchTable.store().manifestFileFactory().create();

            List<ManifestEntry> appendTableFiles = new ArrayList<>();
            List<ManifestEntry> appendChangelog = new ArrayList<>();
            List<IndexManifestEntry> appendHashIndexFiles =
                    branchTable
                            .store()
                            .indexManifestFileFactory()
                            .create()
                            .read(cherryPickSnapshot.indexManifest());

            // 读取 append 文件.
            readAndUpdateManifestEntry(
                    manifestFileReader,
                    manifestListReader.readDeltaManifests(cherryPickSnapshot),
                    appendTableFiles,
                    updatedSchema);

            // 读取 change log
            readAndUpdateManifestEntry(
                    manifestFileReader,
                    manifestListReader.readChangelogManifests(cherryPickSnapshot),
                    appendChangelog,
                    updatedSchema);

            FileStoreCommitImpl fileStoreCommit =
                    (FileStoreCommitImpl)
                            masterTable
                                    .store()
                                    .newCommit(cherryPickSnapshot.commitUser(), masterTable);
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
            updatedSnapshot = masterTable.store().snapshotManager().latestSnapshot();
        } catch (Throwable e) {
            if (updatedSchema != null) {
                Long latestSnpId = masterTable.store().snapshotManager().latestSnapshotId();
                if (latestSnpId != null) {
                    if (!oldSnapshot.isPresent() || oldSnapshot.get().id() < latestSnpId) {
                        masterTable
                                .fileIO()
                                .deleteQuietly(
                                        masterTable
                                                .schemaManager()
                                                .toSchemaPath(updatedSchema.id()));
                    }
                }
            }
            throw e;
        }
        return updatedSnapshot;
    }

    public VersionControlOperator overwriteOptions(boolean overwriteOptions) {
        this.overwriteOptions = overwriteOptions;
        return this;
    }

    private void readAndUpdateManifestEntry(
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
