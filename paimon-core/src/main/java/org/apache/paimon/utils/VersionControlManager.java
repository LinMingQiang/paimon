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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Version control operator. */
public class VersionControlManager {

    protected final FileStoreTable targetTable;
    protected final CatalogEnvironment catalogEnvironment;

    protected boolean overwriteOptions;

    protected boolean mergeSchema;

    public VersionControlManager(
            FileStoreTable targetTable, CatalogEnvironment catalogEnvironment) {
        this.catalogEnvironment = catalogEnvironment;
        this.targetTable = targetTable;
    }

    /** Cherry-pick snapshot from branch to current branch. */
    public Snapshot cherryPick(String fromBranch, long snapshotId) {
        FileStoreTable fromTable = targetTable.switchToBranch(fromBranch);

        validateForCherryPick();

        Snapshot currentSnapshot = targetTable.snapshotManager().latestSnapshot();
        TableSchema currentSchema = targetTable.schemaManager().latest().orElse(null);

        TableSchema appeliedSchema = null;
        boolean hasCommitNewSchema = false;
        Snapshot appeliedSnapshot;
        try {

            // TODO : 需要增加一个检测，当前 cherry pick 的 file 是否已经存在在 main 了, 也就是一个 数据被 cp 了多次.
            // TODO : 需要测试 AddPartitionCommitCallback，这个在 FileStoreCommitImpl.commit 的时候会 call back.
            Snapshot cherryPickSnapshot = getCherryPickSnapshot(fromTable, snapshotId);
            String commitUser = cherryPickSnapshot.commitUser();

            appeliedSchema = mergeSchema(currentSchema, fromTable.schema());

            if (appeliedSchema != null) {
                if (currentSchema == null || !currentSchema.equals(appeliedSchema)) {
                    targetTable.schemaManager().commit(appeliedSchema);
                    hasCommitNewSchema = true;
                }
            }

            FileStoreCommitImpl fileStoreCommit =
                    (FileStoreCommitImpl) targetTable.store().newCommit(commitUser, targetTable);
            fileStoreCommit.commit(
                    createManifestCommittable(fromTable, appeliedSchema, cherryPickSnapshot),
                    false);
            fileStoreCommit.close();
            appeliedSnapshot = targetTable.store().snapshotManager().latestSnapshot();

        } catch (Throwable e) {
            if (hasCommitNewSchema) {
                fallBackCherryPick(appeliedSchema, currentSnapshot, currentSchema);
            }
            throw new RuntimeException("Cherry-pick is failed.", e);
        }

        return appeliedSnapshot;
    }

    @VisibleForTesting
    public TableSchema mergeSchema(TableSchema oldSchema, TableSchema branchSchema)
            throws Exception {

        if (oldSchema == null) {
            return branchSchema;
        }

        TableSchema updatedSchema = null;
        Optional<TableSchema> mergedSchema =
                targetTable
                        .schemaManager()
                        .mergeSchema(oldSchema, branchSchema, overwriteOptions, true);

        if (mergedSchema.isPresent()) {
            updatedSchema = mergedSchema.get();
            Preconditions.checkState(
                    updatedSchema.id() - 1 == oldSchema.id(), "schema id has been changed.");
        }
        return updatedSchema;
    }

    @VisibleForTesting
    public void fallBackCherryPick(
            TableSchema updatedSchema, Snapshot currentSnapshot, TableSchema beforeSchema) {
        Snapshot latestSnp = targetTable.store().snapshotManager().latestSnapshot();
        if (latestSnp == null) {
            // new Schema has not been used.
            targetTable
                    .fileIO()
                    .deleteQuietly(targetTable.schemaManager().toSchemaPath(updatedSchema.id()));
        } else {
            if (latestSnp.schemaId() >= updatedSchema.id()) {
                throw new RuntimeException(
                        String.format(
                                "New schema [%s] has been used for snapshot [%s], we can not delete the schema, please rollback to the last snapshot [%s]",
                                updatedSchema.id(), latestSnp.id(), currentSnapshot.id()));
            }
        }
    }

    public VersionControlManager overwriteOptions(boolean overwriteOptions) {
        this.overwriteOptions = overwriteOptions;
        return this;
    }

    public VersionControlManager mergeSchema(boolean mergeSchema) {
        this.mergeSchema = mergeSchema;
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

    private ManifestCommittable createManifestCommittable(
            FileStoreTable fromTable, TableSchema updatedSchema, Snapshot cherryPickSnapshot) {

        long commitIdentifier = cherryPickSnapshot.commitIdentifier();
        Long watermark = cherryPickSnapshot.watermark();
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        ManifestList manifestListReader = fromTable.store().manifestListFactory().create();
        ManifestFile manifestFileReader = fromTable.store().manifestFileFactory().create();

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

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(commitIdentifier, watermark);
        if (cherryPickSnapshot.properties() != null) {
            cherryPickSnapshot.properties().forEach(manifestCommittable::addProperty);
        }
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> appendDataFiles =
                appendTableFiles.stream()
                        .collect(Collectors.groupingBy(x -> Pair.of(x.partition(), x.bucket())));

        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> changelogDataFiles =
                appendChangelog.stream()
                        .collect(Collectors.groupingBy(x -> Pair.of(x.partition(), x.bucket())));

        Set<Pair<BinaryRow, Integer>> allPartitionAndBucket = new HashSet<>();
        allPartitionAndBucket.addAll(appendDataFiles.keySet());
        allPartitionAndBucket.addAll(changelogDataFiles.keySet());

        for (Pair<BinaryRow, Integer> partitionBucketPair : allPartitionAndBucket) {
            List<ManifestEntry> newFiles =
                    appendDataFiles.getOrDefault(partitionBucketPair, Collections.emptyList());
            List<ManifestEntry> changelogFiles =
                    changelogDataFiles.getOrDefault(partitionBucketPair, Collections.emptyList());
            DataIncrement dataIncrement =
                    new DataIncrement(
                            newFiles.stream().map(ManifestEntry::file).collect(Collectors.toList()),
                            Collections.emptyList(),
                            changelogFiles.stream()
                                    .map(ManifestEntry::file)
                                    .collect(Collectors.toList()));

            CommitMessageImpl commitMessage =
                    new CommitMessageImpl(
                            partitionBucketPair.getLeft(),
                            partitionBucketPair.getRight(),
                            null,
                            dataIncrement,
                            CompactIncrement.emptyIncrement(),
                            IndexIncrement.emptyIncrement());
            manifestCommittable.addFileCommittable(commitMessage);
        }

        return manifestCommittable;
    }

    private Snapshot getCherryPickSnapshot(FileStoreTable fromTable, long snapshotId) {
        Preconditions.checkArgument(
                fromTable.snapshotManager().snapshotExists(snapshotId),
                "Cherry-pick snapshot id %s not found.",
                snapshotId);

        Snapshot cherryPickSnapshot = fromTable.snapshot(snapshotId);
        Preconditions.checkArgument(
                cherryPickSnapshot.commitKind() == Snapshot.CommitKind.APPEND,
                "Cherry-pick can only pick snapshots of APPEND CommitKind.");
        return cherryPickSnapshot;
    }

    /** Check whether the current table supports cherry-pick. */
    private void validateForCherryPick() {
        Preconditions.checkArgument(
                targetTable.primaryKeys().isEmpty()
                        || targetTable.bucketMode() == BucketMode.HASH_FIXED,
                "Cherry-pick is only supported in append-only or hash-fixed primary key table.");
        Preconditions.checkArgument(
                !targetTable.coreOptions().needLookup(), "Cherry-pick do not support lookup mode.");
    }
}
