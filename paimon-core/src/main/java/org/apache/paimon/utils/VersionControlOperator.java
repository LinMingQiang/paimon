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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
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
    protected final CatalogEnvironment catalogEnvironment;

    protected boolean overwriteOptions;

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
                "Cherry-pick is only supported in APPEND commitKind snapshot.");

        Preconditions.checkArgument(
                masterTable.primaryKeys().isEmpty()
                        || masterTable.bucketMode() == BucketMode.HASH_FIXED,
                "Cherry-pick is only supported in append-only or hash-fixed primary key table.");

        Preconditions.checkArgument(
                masterTable.primaryKeys().isEmpty()
                        || masterTable.coreOptions().changelogProducer()
                                == CoreOptions.ChangelogProducer.INPUT,
                "Cherry-pick is only supported in append-only table or primary key table with INPUT changelogProducer.");

        Optional<Snapshot> oldSnapshot = masterTable.latestSnapshot();
        TableSchema baseSchema = masterTable.schemaManager().latest().get();

        TableSchema pickSchema = branchTable.schemaManager().schema(cherryPickSnapshot.schemaId());
        TableSchema updatedSchema = null;
        Snapshot updatedSnapshot;
        try {

            updatedSchema = mergeSchemaAndCommit(baseSchema, pickSchema);

            ManifestList manifestListReader = branchTable.store().manifestListFactory().create();
            ManifestFile manifestFileReader = branchTable.store().manifestFileFactory().create();

            List<ManifestEntry> appendTableFiles = new ArrayList<>();
            List<ManifestEntry> appendChangelog = new ArrayList<>();

            // Read append data files.
            readAndUpdateManifestEntry(
                    manifestFileReader,
                    manifestListReader.readDeltaManifests(cherryPickSnapshot),
                    appendTableFiles,
                    updatedSchema);

            // Read append change-log data files.
            readAndUpdateManifestEntry(
                    manifestFileReader,
                    manifestListReader.readChangelogManifests(cherryPickSnapshot),
                    appendChangelog,
                    updatedSchema);

            updatedSnapshot =
                    commitToTargetMaster(
                            appendTableFiles,
                            appendChangelog,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            cherryPickSnapshot);

        } catch (Throwable e) {
            fallBackCherryPick(updatedSchema, oldSnapshot.orElse(null));
            throw new RuntimeException("cherryPick failed.", e);
        }
        return updatedSnapshot;
    }

    @VisibleForTesting
    public Snapshot commitToTargetMaster(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelog,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelog,
            List<IndexManifestEntry> appendHashIndexFiles,
            List<IndexManifestEntry> compactDvIndexFiles,
            Snapshot baseSnapshot) {

        FileStoreCommitImpl fileStoreCommit =
                (FileStoreCommitImpl)
                        masterTable.store().newCommit(baseSnapshot.commitUser(), masterTable);
        fileStoreCommit.commit(
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles,
                baseSnapshot.commitIdentifier(),
                baseSnapshot.watermark(),
                baseSnapshot.logOffsets(),
                false);

        fileStoreCommit.close();
        return masterTable.store().snapshotManager().latestSnapshot();
    }

    @VisibleForTesting
    public TableSchema mergeSchemaAndCommit(TableSchema oldSchema, TableSchema branchSchema)
            throws Exception {
        TableSchema updatedSchema = null;
        Optional<TableSchema> mergedSchema =
                masterTable
                        .schemaManager()
                        .mergeSchema(oldSchema, branchSchema, overwriteOptions, true);

        // Commit new schema.
        if (mergedSchema.isPresent() && masterTable.schemaManager().commit(mergedSchema.get())) {
            updatedSchema = mergedSchema.get();
            Preconditions.checkState(
                    updatedSchema.id() - 1 == oldSchema.id(), "schema id has been changed.");
        }
        return updatedSchema;
    }

    @VisibleForTesting
    public void fallBackCherryPick(TableSchema updatedSchema, Snapshot beforeSnapshot) {
        Snapshot latestSnp = masterTable.store().snapshotManager().latestSnapshot();
        if (updatedSchema != null && latestSnp != null) {
            // newSchema has not been use, we need to delete the updatedSchema.
            if (beforeSnapshot != null && beforeSnapshot.schemaId() == latestSnp.schemaId()) {
                masterTable
                        .fileIO()
                        .deleteQuietly(
                                masterTable.schemaManager().toSchemaPath(updatedSchema.id()));
            }
        }
    }

    public VersionControlOperator overwriteOptions(boolean overwriteOptions) {
        this.overwriteOptions = overwriteOptions;
        return this;
    }

    /** Read ManifestEntry from ManifestFile and update schemaId if necessary. */
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
