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
import org.apache.paimon.branch.TableBranch;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BranchDeletion;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.FileUtils.listOriginalVersionedFiles;
import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Branch}. */
public class BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(BranchManager.class);

    public static final String BRANCH_PREFIX = "branch-";
    public static final String DEFAULT_MAIN_BRANCH = "main";

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;

    private final BranchDeletion branchDeletion;

    public BranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager,
            BranchDeletion branchDeletion) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
        this.branchDeletion = branchDeletion;
    }

    /** Return the root Directory of branch. */
    public Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    /** Return the root Directory of branch by given tablePath. */
    public static Path branchDirectory(Path tablePath) {
        return new Path(tablePath + "/branch");
    }

    public static List<String> branchNames(FileIO fileIO, Path tablePath) throws IOException {
        return listOriginalVersionedFiles(fileIO, branchDirectory(tablePath), BRANCH_PREFIX)
                .collect(Collectors.toList());
    }

    public static boolean isMainBranch(String branch) {
        return branch.equals(DEFAULT_MAIN_BRANCH);
    }

    /** Return the path string of a branch. */
    public static String branchPath(Path tablePath, String branch) {
        return isMainBranch(branch)
                ? tablePath.toString()
                : tablePath.toString() + "/branch/" + BRANCH_PREFIX + branch;
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(branchPath(tablePath, branchName));
    }

    /** Create empty branch. */
    public void createBranch(String branchName) {
        checkArgument(
                !isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);

        try {
            TableSchema latestSchema = schemaManager.latest().get();
            fileIO.copyFile(
                    schemaManager.toSchemaPath(latestSchema.id()),
                    schemaManager.copyWithBranch(branchName).toSchemaPath(latestSchema.id()),
                    true);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    public void createBranch(String branchName, long snapshotId) {
        checkArgument(
                !isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);

        Snapshot snapshot = snapshotManager.snapshot(snapshotId);

        try {
            // Copy the corresponding snapshot and schema files into the branch directory
            fileIO.copyFile(
                    snapshotManager.snapshotPath(snapshotId),
                    snapshotManager.copyWithBranch(branchName).snapshotPath(snapshot.id()),
                    true);
            fileIO.copyFile(
                    schemaManager.toSchemaPath(snapshot.schemaId()),
                    schemaManager.copyWithBranch(branchName).toSchemaPath(snapshot.schemaId()),
                    true);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    public void createBranch(String branchName, String tagName) {
        checkArgument(
                !isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be created.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
        checkArgument(tagManager.tagExists(tagName), "Tag name '%s' not exists.", tagName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);

        Snapshot snapshot = tagManager.taggedSnapshot(tagName);

        try {
            // Copy the corresponding tag, snapshot and schema files into the branch directory
            fileIO.copyFile(
                    tagManager.tagPath(tagName),
                    tagManager.copyWithBranch(branchName).tagPath(tagName),
                    true);
            fileIO.copyFile(
                    snapshotManager.snapshotPath(snapshot.id()),
                    snapshotManager.copyWithBranch(branchName).snapshotPath(snapshot.id()),
                    true);
            fileIO.copyFile(
                    schemaManager.toSchemaPath(snapshot.schemaId()),
                    schemaManager.copyWithBranch(branchName).toSchemaPath(snapshot.schemaId()),
                    true);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    public void deleteBranch(String branchName) {
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        try {

            if (!branchExists(branchName)) {
                return;
            }

            Snapshot snapshotToClean =
                    snapshotManager.copyWithBranch(branchName).earliestSnapshot();

            if (snapshotToClean != null) {
                if (!snapshotManager.snapshotExists(snapshotToClean.id())) {
                    SortedMap<Snapshot, List<TableBranch>> branchReferenceSnapshotsMap =
                            branchesCreateSnapshots();
                    // If the snapshotToClean is not referenced by other branches or tags, we need
                    // to do clean the dataFiles and manifestFiles.
                    if (branchReferenceSnapshotsMap
                                            .getOrDefault(snapshotToClean, Collections.emptyList())
                                            .size()
                                    == 1
                            && !tagManager.taggedSnapshots().contains(snapshotToClean)) {
                        // do clean.
                        doClean(
                                snapshotToClean,
                                SnapshotManager.mergeTreeSetToList(
                                        branchReferenceSnapshotsMap.keySet(),
                                        tagManager.taggedSnapshots()));
                    }
                }
            }

            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            branchPath(tablePath, branchName)),
                    e);
        }
    }

    public void doClean(Snapshot snapshotToDelete, List<Snapshot> referencedSnapshots) {
        // collect skipping sets from the left neighbor branch and the nearest right neighbor
        // (either the earliest snapshot or right neighbor branch)
        List<Snapshot> skippedSnapshots =
                SnapshotManager.findNearestNeighborsSnapshot(
                        snapshotToDelete, referencedSnapshots, snapshotManager);

        // delete data files and empty directories
        Predicate<ManifestEntry> dataFileSkipper = null;
        boolean success = true;
        try {
            dataFileSkipper = branchDeletion.dataFileSkipper(skippedSnapshots);
        } catch (Exception e) {
            LOG.info(
                    String.format(
                            "Skip cleaning data files for branch of snapshot %s due to failed to build skipping set.",
                            snapshotToDelete.id()),
                    e);
            success = false;
        }
        if (success) {
            branchDeletion.cleanUnusedDataFiles(snapshotToDelete, dataFileSkipper);
            branchDeletion.cleanEmptyDirectories();
        }

        // delete manifests
        branchDeletion.cleanUnusedManifests(
                snapshotToDelete, branchDeletion.manifestSkippingSet(skippedSnapshots));
    }

    /** Check if path exists. */
    public boolean fileExists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    public void fastForward(String branchName) {
        checkArgument(
                !branchName.equals(DEFAULT_MAIN_BRANCH),
                "Branch name '%s' do not use in fast-forward.",
                branchName);
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        Long earliestSnapshotId = snapshotManager.copyWithBranch(branchName).earliestSnapshotId();
        Snapshot earliestSnapshot =
                snapshotManager.copyWithBranch(branchName).snapshot(earliestSnapshotId);
        long earliestSchemaId = earliestSnapshot.schemaId();

        try {
            // Delete snapshot, schema, and tag from the main branch which occurs after
            // earliestSnapshotId
            List<Path> deleteSnapshotPaths =
                    listVersionedFileStatus(
                                    fileIO, snapshotManager.snapshotDirectory(), "snapshot-")
                            .map(FileStatus::getPath)
                            .filter(
                                    path ->
                                            Snapshot.fromPath(fileIO, path).id()
                                                    >= earliestSnapshotId)
                            .collect(Collectors.toList());
            List<Path> deleteSchemaPaths =
                    listVersionedFileStatus(fileIO, schemaManager.schemaDirectory(), "schema-")
                            .map(FileStatus::getPath)
                            .filter(
                                    path ->
                                            TableSchema.fromPath(fileIO, path).id()
                                                    >= earliestSchemaId)
                            .collect(Collectors.toList());
            List<Path> deleteTagPaths =
                    listVersionedFileStatus(fileIO, tagManager.tagDirectory(), "tag-")
                            .map(FileStatus::getPath)
                            .filter(
                                    path ->
                                            Snapshot.fromPath(fileIO, path).id()
                                                    >= earliestSnapshotId)
                            .collect(Collectors.toList());

            List<Path> deletePaths =
                    Stream.concat(
                                    Stream.concat(
                                            deleteSnapshotPaths.stream(),
                                            deleteSchemaPaths.stream()),
                                    deleteTagPaths.stream())
                            .collect(Collectors.toList());

            // Delete latest snapshot hint
            snapshotManager.deleteLatestHint();

            fileIO.deleteFilesQuietly(deletePaths);
            fileIO.copyFiles(
                    snapshotManager.copyWithBranch(branchName).snapshotDirectory(),
                    snapshotManager.snapshotDirectory(),
                    true);
            fileIO.copyFiles(
                    schemaManager.copyWithBranch(branchName).schemaDirectory(),
                    schemaManager.schemaDirectory(),
                    true);
            fileIO.copyFiles(
                    tagManager.copyWithBranch(branchName).tagDirectory(),
                    tagManager.tagDirectory(),
                    true);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when fast forward '%s' (directory in %s).",
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    /** Get all branches for the table. */
    public List<String> branches() {
        try {
            return listVersionedDirectories(fileIO, branchDirectory(), BRANCH_PREFIX)
                    .map(status -> status.getPath().getName().substring(BRANCH_PREFIX.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get all snapshots that are referenced by branches. */
    public SortedMap<Snapshot, List<String>> branchesCreateSnapshots() {
        TreeMap<Snapshot, List<String>> sortedSnapshots =
                new TreeMap<>(Comparator.comparingLong(Snapshot::id));

        for (String branchName : branches()) {
            Snapshot branchCreateSnapshot =
                    snapshotManager.copyWithBranch(branchName).earliestSnapshot();
            if (branchCreateSnapshot == null) {
                // Support empty branch.
                branchSnapshots.put(new TableBranch(branchName, path.getValue()), null);
                continue;
            }
            FileStoreTable branchTable =
                    FileStoreTableFactory.create(
                            fileIO, new Path(branchPath(tablePath, branchName)));
            SortedMap<Snapshot, List<String>> snapshotTags = branchTable.tagManager().tags();
            Snapshot earliestSnapshot = branchTable.snapshotManager().earliestSnapshot();
            if (snapshotTags.isEmpty()) {
                // Create based on snapshotId.
                branchSnapshots.put(
                        new TableBranch(branchName, earliestSnapshot.id(), path.getValue()),
                        earliestSnapshot);
            } else {
                Snapshot snapshot = snapshotTags.firstKey();
                // current branch is create from tag.
                if (earliestSnapshot.id() == snapshot.id()) {
                    List<String> tags = snapshotTags.get(snapshot);
                    checkArgument(tags.size() == 1);
                    branchSnapshots.put(
                            new TableBranch(
                                    branchName, tags.get(0), snapshot.id(), path.getValue()),
                            snapshot);
                } else {
                    // Create based on snapshotId.
                    branchSnapshots.put(
                            new TableBranch(branchName, earliestSnapshot.id(), path.getValue()),
                            earliestSnapshot);
                }
            }
        }

        for (Map.Entry<String, Snapshot> snapshotEntry : branches().entrySet()) {
            if (snapshotEntry.getValue() != null) {
                sortedSnapshots
                        .computeIfAbsent(snapshotEntry.getValue(), s -> new ArrayList<>())
                        .add(snapshotEntry.getKey());
            }
        }
        return sortedSnapshots;
    }
}
