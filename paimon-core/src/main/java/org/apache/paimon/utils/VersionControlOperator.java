package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class VersionControlOperator {
    public FileStoreTable masterTable;
    public boolean syncCoreOptions;

    public VersionControlOperator(FileStoreTable masterTable) {
        this.masterTable = masterTable;
    }

    public Snapshot cherryPick(String fromBranch,long snapshotId) {
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
                            .mergeSchema(
                                    oldSchema,
                                    branchSchema,
                                    syncCoreOptions,
                                    true);
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
                            masterTable.store().newCommit(cherryPickSnapshot.commitUser(), masterTable);
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
                                        masterTable.schemaManager().toSchemaPath(updatedSchema.id()));
                    }
                }
            }
            throw e;
        }
        return updatedSnapshot;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public long snapshotId;
        public FileStoreTable masterTable;
        public FileStoreTable branchTable;
        public boolean syncCoreOptions;

        public Builder cherryPickSnapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder masterTable(FileStoreTable masterTable) {
            this.masterTable = masterTable;
            return this;
        }

        public Builder syncCoreOptions(boolean syncCoreOptions) {
            this.syncCoreOptions = syncCoreOptions;
            return this;
        }

        public VersionControlOperator build() {
            return new VersionControlOperator(masterTable);
        }
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
