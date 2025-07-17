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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
        return mergePick(fromBranch, Collections.singletonList(snapshotId));
    }

    private TableSchema mergeCherryPickSchemas(
            FileStoreTable fromTable,
            List<Snapshot> cherryPickSnapshotList,
            TableSchema currentSchema)
            throws Exception {
        List<TableSchema> cherryPickSchemas =
                cherryPickSnapshotList.stream()
                        .map(snapshot -> fromTable.schemaManager().schema(snapshot.schemaId()))
                        .collect(Collectors.toList());
        return mergeMultiSchema(currentSchema, cherryPickSchemas);
    }

    public Snapshot mergePick(String fromBranch, long fromSnapshotId, long toSnapshotId) {
        return mergePick(
                fromBranch,
                LongStream.rangeClosed(fromSnapshotId, toSnapshotId)
                        .boxed()
                        .collect(Collectors.toList()));
    }

    public Snapshot mergePick(String fromBranch, List<Long> snapshotLists) {
        FileStoreTable fromTable = getBranchFileStoreTable(fromBranch);

        checkApplicability();

        Snapshot currentSnapshot = masterTable.snapshotManager().latestSnapshot();
        TableSchema currentSchema = masterTable.schemaManager().latest().get();

        TableSchema appeliedSchema = null;
        Snapshot appeliedSnapshot;
        try {

            // TODO : 需要增加一个检测，当前 cherry pick 的 file 是否已经存在在 main 了, 也就是一个 数据被 cp 了多次.
            List<Snapshot> cherryPickSnapshotList =
                    snapshotLists.stream()
                            .map(snapshotId -> getCherryPickSnapshot(fromTable, snapshotId))
                            .collect(Collectors.toList());
            String commitUser = cherryPickSnapshotList.get(0).commitUser();

            appeliedSchema =
                    mergeCherryPickSchemas(fromTable, cherryPickSnapshotList, currentSchema);

            FileStoreCommitImpl fileStoreCommit =
                    (FileStoreCommitImpl) masterTable.store().newCommit(commitUser, masterTable);
            fileStoreCommit.commit(
                    createManifestCommittable(fromTable, appeliedSchema, cherryPickSnapshotList),
                    false);
            fileStoreCommit.close();
            appeliedSnapshot = masterTable.store().snapshotManager().latestSnapshot();

        } catch (Throwable e) {
            fallBackCherryPick(appeliedSchema, currentSnapshot);
            throw new RuntimeException("cherryPick failed.", e);
        }
        return appeliedSnapshot;
    }

    @VisibleForTesting
    public TableSchema mergeSchema(TableSchema oldSchema, TableSchema branchSchema)
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
    public TableSchema mergeMultiSchema(TableSchema oldSchema, List<TableSchema> branchSchemas)
            throws Exception {
        TableSchema mergedSchema = oldSchema;
        for (TableSchema branchSchema : branchSchemas) {
            TableSchema temp = mergeSchema(mergedSchema, branchSchema);
            if (temp != null) {
                mergedSchema = temp;
            }
        }
        return mergedSchema;
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

    private ManifestCommittable createManifestCommittable(
            FileStoreTable fromTable,
            TableSchema updatedSchema,
            List<Snapshot> cherryPickSnapshotList)
            throws Exception {

        long commitIdentifier = cherryPickSnapshotList.get(0).commitIdentifier();
        Long watermark = null;
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        ManifestList manifestListReader = fromTable.store().manifestListFactory().create();
        ManifestFile manifestFileReader = fromTable.store().manifestFileFactory().create();
        Map<String, String> properties = new HashMap<>();

        for (Snapshot cherryPickSnapshot : cherryPickSnapshotList) {
            commitIdentifier =
                    cherryPickSnapshot.commitIdentifier() > commitIdentifier
                            ? commitIdentifier
                            : cherryPickSnapshot.commitIdentifier();
            if (watermark == null
                    || (cherryPickSnapshot.watermark() != null
                            && cherryPickSnapshot.watermark() > watermark)) {
                watermark = cherryPickSnapshot.watermark();
            }
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
            if (cherryPickSnapshot.properties() != null) {
                properties.putAll(cherryPickSnapshot.properties());
            }
        }

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(commitIdentifier, watermark);
        properties.forEach(manifestCommittable::addProperty);

        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> appendDataFiles =
                appendTableFiles.stream()
                        .collect(Collectors.groupingBy(x -> Pair.of(x.partition(), x.bucket())));
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> changelogDataFiles =
                appendChangelog.stream()
                        .collect(Collectors.groupingBy(x -> Pair.of(x.partition(), x.bucket())));

        appendDataFiles.forEach(
                (k, v) -> {
                    DataIncrement dataIncrement =
                            new DataIncrement(
                                    v.stream()
                                            .map(ManifestEntry::file)
                                            .collect(Collectors.toList()),
                                    Collections.emptyList(),
                                    Collections.emptyList());
                    CommitMessageImpl commitMessage =
                            new CommitMessageImpl(
                                    k.getKey(),
                                    k.getValue(),
                                    null,
                                    dataIncrement,
                                    CompactIncrement.emptyIncrement(),
                                    IndexIncrement.emptyIncrement());
                    manifestCommittable.addFileCommittable(commitMessage);
                });

        changelogDataFiles.forEach(
                (k, v) -> {
                    DataIncrement dataIncrement =
                            new DataIncrement(
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    v.stream()
                                            .map(ManifestEntry::file)
                                            .collect(Collectors.toList()));
                    CommitMessageImpl commitMessage =
                            new CommitMessageImpl(
                                    k.getKey(),
                                    k.getValue(),
                                    null,
                                    dataIncrement,
                                    CompactIncrement.emptyIncrement(),
                                    IndexIncrement.emptyIncrement());
                    manifestCommittable.addFileCommittable(commitMessage);
                });

        return manifestCommittable;
    }

    private FileStoreTable getBranchFileStoreTable(String branchName) {
        Preconditions.checkArgument(
                masterTable.branchManager().branchExists(branchName),
                "Branch %s is not exist.",
                branchName);
        return masterTable.switchToBranch(branchName);
    }

    private Snapshot getCherryPickSnapshot(FileStoreTable fromTable, long snapshotId) {
        Preconditions.checkArgument(
                fromTable.snapshotManager().snapshotExists(snapshotId),
                "Cherry-pick snapshot id %s not found.",
                snapshotId);

        Snapshot cherryPickSnapshot = fromTable.snapshot(snapshotId);
        Preconditions.checkArgument(
                cherryPickSnapshot.commitKind() == Snapshot.CommitKind.APPEND,
                "Cherry-pick is only supported in APPEND commitKind snapshot.");
        return cherryPickSnapshot;
    }

    /** 检验 target table 的可应用型. */
    private void checkApplicability() {
        Preconditions.checkArgument(
                masterTable.primaryKeys().isEmpty()
                        || masterTable.bucketMode() == BucketMode.HASH_FIXED,
                "Cherry-pick is only supported in append-only or hash-fixed primary key table.");
        Preconditions.checkArgument(
                !masterTable.coreOptions().needLookup(), "Cherry-pick do not support lookup mode.");
    }
}
