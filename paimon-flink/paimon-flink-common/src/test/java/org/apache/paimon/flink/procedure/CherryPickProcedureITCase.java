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
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link CherryPickSnapshotProcedure }. */
public class CherryPickProcedureITCase extends CatalogITCaseBase {

    @Override
    @BeforeEach
    public void before() throws IOException {
        options.put("cache-enabled", "false");
        super.before();
    }

    public Map<String, String> getCoreOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("write-only", "true");
        options.put("merge-engine", "partial-update");
        options.put("changelog-producer", "input");
        return options;
    }

    @Test
    public void testCreateBranchFromOtherBranch() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable branchTable = paimonTable("T$branch_test");
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        sql("CALL sys.create_tag('default.T$branch_test', 'test_tag', 2)");

        String checkOutFromOtherBranch = "checkout_from_test_branch";
        sql(
                "CALL sys.create_branch('default.T$branch_test', '%s', 'test_tag')",
                checkOutFromOtherBranch);
        // The data is branch test data.
        assertThat(collectResult("SELECT * FROM `T$branch_checkout_from_test_branch`"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt]");

        // insert to another branch.
        sql(
                "INSERT INTO `T$branch_checkout_from_test_branch` VALUES "
                        + "(1, 'checkout_from_test_branch', 'pt')");
        FileStoreTable anotherBranchTable = paimonTable("T$branch_checkout_from_test_branch");
        assertThat(anotherBranchTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        assertThat(collectResult("SELECT * FROM `T$branch_checkout_from_test_branch`"))
                .containsExactlyInAnyOrder("+I[1, checkout_from_test_branch, pt]");
    }

    @Test
    public void testCherryPickToMain() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        cherryPick("default.T", "test", "main", 2, false);
        FileStoreTable mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt]");
    }

    @Test
    public void testCherryPickMultiPartitions() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'updated-by-branch', 'pt'),(2, 'branch_data', 'pt2')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        cherryPick("default.T", "test", "main", 2, false);
        FileStoreTable mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, updated-by-branch, pt]",
                        "+I[2, branch_data, pt2]");
    }

    @Test
    public void testCherryPickSnapshotIdNotExist() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThatThrownBy(() -> cherryPick("default.T", "test", "main", 3, false))
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class, "Cherry-pick snapshot id 3 not found."));
    }

    @Test
    public void testCherryPickToAnotherBranch() throws Exception {
        createBranch(true, "from_branch", getCoreOptions());
        assertThat(paimonTable("T").snapshotManager().latestSnapshotId()).isEqualTo(1);

        sql("CALL sys.create_branch('default.T', 'target_branch', 'tag1')");

        FileStoreTable fromBranch = paimonTable("T$branch_from_branch");
        // 这个记录是为了，让此分支的 pick 的数据 seq num 大于 target_branch 的.
        sql("INSERT INTO `T$branch_from_branch` VALUES " + "(0, 'ignore', 'pt')");
        sql("INSERT INTO `T$branch_from_branch` VALUES " + "(1, 'from_branch', 'pt')");
        assertThat(fromBranch.snapshotManager().latestSnapshotId()).isEqualTo(3);

        FileStoreTable targetBranchTable = paimonTable("T$branch_target_branch");
        sql("INSERT INTO `T$branch_target_branch` VALUES " + "(1, 'target_branch', 'pt')");
        assertThat(targetBranchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        cherryPick("default.T", "from_branch", "target_branch", 3, false);
        targetBranchTable = paimonTable("T$branch_target_branch");
        assertThat(targetBranchTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        assertThat(collectResult("SELECT * FROM T$branch_target_branch"))
                .containsExactlyInAnyOrder("+I[1, from_branch, pt]");
    }

    @Test
    public void testOverwriteOptions() {}

    /** 修改 option 的不可变更参数，在合并时应该失败. */
    @Test
    public void testChangeImmutableOptions() {}

    /**
     * 使用默认 sequence.field 时，在数据去重时是按数据文件里面的 seq num 来决定的, 正常情况下这个 seq num 是递增的，但是如果你是并发写，或者 pick
     * 的这种情况，他 seq 是会重复的，那他就不准确了，seq 是会重复的. 想要达到按照 snapshot 的排序效果，有两种方式，一种是 设置Sequence field.
     * 一种是提供一个方式，修改 data file 的时间为快照时间. 如果是指定 Sequence field，应该是没有问题的. 这是 sequence 排序的
     * bug，多流并发写的时候，不是按照 快照先后也不是按照文件生成先后，而是这个 seq. 所以在推荐里面： merge-engine=partial-update ， 解决增加字段导致的
     * null 问题. 配置 sequence.field 解决排序问题.
     */
    @Test
    public void testDataSortingBySequenceNum() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        // 先写入 主分支数据.
        sql("INSERT INTO T VALUES" + " (1, 'apple-2', 'pt')");

        // 再写入分支数据
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        // 然后 pick 分支数据
        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        // 按道理，这个排序顺序应该是分支的数据才对。但是实际他是按 seq num 来排序的.
        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, apple-2, pt]");
    }

    /** 指定排序字段，测试 pick 之后是否还是有效的. */
    @Test
    public void testSpecifySequenceField() throws Exception {
        StringBuilder sb = new StringBuilder();
        Map<String, String> options = getCoreOptions();
        // 不加这个会报错
        options.put("sequence.field", "seq_field");
        options.forEach((k, v) -> sb.append(String.format(",'%s'='%s'", k, v)));
        sql(
                "CREATE TABLE T ("
                        + " k INT"
                        + ", v STRING"
                        + ", seq_field STRING"
                        + ", pt STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + "%s"
                        + " ) ",
                sb.substring(1, sb.toString().length()));

        sql("INSERT INTO T VALUES" + " (1, 'apple', '1' ,'pt')");
        sql("CALL sys.create_tag('default.T', 'tag1', 1)");
        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");

        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        // 先写入 主分支数据.
        sql("INSERT INTO T VALUES" + " (1, 'apple-2', '2', 'pt')");

        // 再写入分支数据
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', '3', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        // 然后 pick 分支数据
        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        // 根据 seq field 排序.
        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, 3, pt]");
    }

    @Test
    public void testCherryPickWithBranchAddCol() throws Exception {

        createBranch(true, getCoreOptions());
        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        // Add v2 column for branch table.
        sql("ALTER TABLE `T$branch_test` ADD (v2 STRING)");
        sql("INSERT INTO `T$branch_test` VALUES(1, 'branch-apple', 'pt', 'branch_col_value')");

        assertThat(collectResult("SELECT * FROM `T$branch_test`"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt, branch_col_value]");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        assertThat(branchTable.schema().fields().size()).isEqualTo(4);

        sql("INSERT INTO T VALUES" + " (1, 'main-apple', 'pt')");

        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(3);
        assertThat(mainTable.schema().fields().size()).isEqualTo(4);

        // 因为 主分支在最后写入数据，所以 分支新增字段的数据会被 null 覆盖，要想有，需要用部分更新策略.
        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, main-apple, pt, branch_col_value]");
    }

    @Test
    public void testCherryPickWithSchemaMerge() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        // Add v2 column for branch table.
        sql("ALTER TABLE `T$branch_test` ADD (branch_col STRING)");
        sql("INSERT INTO `T$branch_test` VALUES(1, 'branch-apple', 'pt', 'branch_col_value')");

        assertThat(collectResult("SELECT * FROM `T$branch_test`"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt, branch_col_value]");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        assertThat(branchTable.schema().fields().size()).isEqualTo(4);

        sql("ALTER TABLE `T` ADD (main_col STRING)");
        sql("INSERT INTO T VALUES" + " (1, 'apple', 'pt', 'main_col_value')");

        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        assertThat(mainTable.schema().fields().size()).isEqualTo(4);

        // Now, we get :
        // main branch : snp-1,snp-2 (new col main_col)
        // test branch : snp-1,snp-2 (new col branch_col)

        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(3);
        assertThat(mainTable.schema().fields().size()).isEqualTo(5);

        // 因为分支数据要早于 主分支写入，按照写入时间来排序的话 main 分支写入的数据 会导致此字段为 null. 应该使用 部分更新.
        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, apple, pt, main_col_value, branch_col_value]");
    }

    @Test
    public void testSchemaDataTypeConflict() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable mainTable;
        sql("ALTER TABLE `T` ADD (conflict_col DOUBLE)");
        sql("ALTER TABLE `T$branch_test` ADD (conflict_col STRING)");
        sql("INSERT INTO `T$branch_test` VALUES(1, 'branch-apple', 'pt', 'conflict_col_value')");

        // 因为可以隐式转换，所以，conflict_col 为 string 类型.
        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.schema().fields().size()).isEqualTo(4);
        assertThat(mainTable.schema().toSchema().rowType().getField("conflict_col").type())
                .isEqualTo(DataTypes.STRING());

        // 无法转换的将会报错.
        sql("ALTER TABLE `T` ADD (conflict_col2 DATE)");
        sql("ALTER TABLE `T$branch_test` ADD (conflict_col2 INT)");
        sql("INSERT INTO `T$branch_test` VALUES(1, 'branch-apple', 'pt', 'conflict_col_value', 1)");

        assertThatThrownBy(() -> cherryPick("default.T", "test", "main", 3, false))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Failed to merge data types DATE and INT"));
    }

    @Test
    public void testCherryPickChangeLogDataFiles() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        List<ManifestEntry> appendChangelog = new ArrayList<>();
        ManifestFile manifestFileReader = branchTable.store().manifestFileFactory().create();
        ManifestList manifestListReader = branchTable.store().manifestListFactory().create();

        // Read append change-log data files.
        readAndUpdateManifestEntry(
                manifestFileReader,
                manifestListReader.readChangelogManifests(
                        branchTable.snapshotManager().latestSnapshot()),
                appendChangelog);

        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        List<ManifestEntry> mainAppendChangelog = new ArrayList<>();
        ManifestFile mainManifestFileReader = mainTable.store().manifestFileFactory().create();
        ManifestList mainManifestListReader = mainTable.store().manifestListFactory().create();

        // Read append change-log data files.
        readAndUpdateManifestEntry(
                mainManifestFileReader,
                mainManifestListReader.readChangelogManifests(
                        mainTable.snapshotManager().latestSnapshot()),
                mainAppendChangelog);

        assertThat(mainAppendChangelog.size()).isEqualTo(1);

        assertThat(mainAppendChangelog)
                .containsExactlyInAnyOrder(appendChangelog.toArray(new ManifestEntry[0]));
    }

    @ParameterizedTest
    @CsvSource({"-1,INPUT", "1,INPUT", "1,LOOKUP"})
    public void testLimitOfCherryPickSupport(int bucket, String changeLogProducer)
            throws Exception {
        Map<String, String> options = getCoreOptions();
        options.put("bucket", String.valueOf(bucket));
        options.put("changelog-producer", changeLogProducer);
        createBranch(true, options);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        if (bucket == 1) {
            if (changeLogProducer.equals("INPUT")) {
                // Do not support COMPACT CommitKind.
                tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
                sql(
                        "CALL sys.compact(`table` => 'default.T$branch_test', compact_strategy => 'full')");
                branchTable = paimonTable("T$branch_test");
                Snapshot latestSnapshot = branchTable.snapshotManager().latestSnapshot();
                assertThat(latestSnapshot.id()).isEqualTo(3);
                assertThat(latestSnapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
                assertThatThrownBy(() -> cherryPick("default.T", "test", "main", 3, false))
                        .satisfies(
                                anyCauseMatches(
                                        IllegalArgumentException.class,
                                        "Cherry-pick is only supported in APPEND commitKind snapshot."));
            } else {
                assertThatThrownBy(() -> cherryPick("default.T", "test", "main", 2, false))
                        .satisfies(
                                anyCauseMatches(
                                        IllegalArgumentException.class,
                                        "Cherry-pick do not support lookup mode."));
            }

        } else {
            // Do not support dynamic bucket table.
            assertThatThrownBy(() -> cherryPick("default.T", "test", "main", 2, false))
                    .satisfies(
                            anyCauseMatches(
                                    IllegalArgumentException.class,
                                    "Cherry-pick is only supported in append-only or hash-fixed primary key table."));
        }
    }

    @Test
    public void testAppendOnlyTable() throws Exception {
        Map<String, String> options = getCoreOptions();
        options.put("bucket", String.valueOf(-1));
        options.remove("changelog-producer");
        createBranch(false, options);
        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        cherryPick("default.T", "test", "main", 2, false);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, apple, pt]", "+I[1, branch-apple, pt]");
    }

    @Test
    public void testAppendTableWithIndexDatafiles() throws Exception {
        Map<String, String> options = getCoreOptions();
        options.put("bucket", String.valueOf(-1));
        options.remove("changelog-producer");
        options.put("file-index.bloom-filter.columns", "v");
        createBranch(false, options);

        FileStoreTable mainTable = paimonTable("T");
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        List<DataSplit> branchDataSplits =
                branchTable.newSnapshotReader().withSnapshot(2).read().dataSplits();
        // assert data index files.
        assertThat(branchDataSplits.size()).isEqualTo(1);
        List<String> branchDataIndexFiles =
                branchDataSplits.get(0).dataFiles().stream()
                        .flatMap(x -> x.extraFiles().stream())
                        .collect(Collectors.toList());
        assertThat(branchDataIndexFiles.size()).isEqualTo(2);

        cherryPick("default.T", "test", "main", 2, false);

        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        List<DataSplit> mainDataSplits =
                mainTable.newSnapshotReader().withSnapshot(2).read().dataSplits();

        assertThat(branchDataSplits).isEqualTo(mainDataSplits);
    }

    public void createBranch(boolean primaryTable, Map<String, String> options) {
        createBranch(primaryTable, "test", options);
    }

    public void createBranch(boolean primaryTable, String branchName, Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        options.forEach((k, v) -> sb.append(String.format(",'%s'='%s'", k, v)));
        sql(
                "CREATE TABLE T ("
                        + " k INT"
                        + ", v STRING"
                        + ", pt STRING"
                        + "%s"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + "%s"
                        + " )",
                primaryTable ? ", PRIMARY KEY (pt, k) NOT ENFORCED" : "",
                sb.substring(1, sb.toString().length()));

        sql("INSERT INTO T VALUES" + " (1, 'apple', 'pt')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");
        sql("CALL sys.create_branch('default.T', '%s', 'tag1')", branchName);
    }

    private void cherryPick(
            String tableId,
            String fromBranchName,
            String toBranchName,
            Integer snapshot,
            Boolean overwriteOptions) {
        sql(
                "CALL sys.cherry_pick(`table` => '%s',"
                        + "`from_branch` =>'%s', "
                        + "`to_branch` =>'%s', "
                        + "`snapshot` => %s,"
                        + "`overwriteOptions` => %s)",
                tableId, fromBranchName, toBranchName, snapshot, overwriteOptions);
    }

    private List<String> collectResult(String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    /** Read ManifestEntry from ManifestFile and update schemaId if necessary. */
    private void readAndUpdateManifestEntry(
            ManifestFile manifestFileReader,
            List<ManifestFileMeta> manifestFileMetas,
            List<ManifestEntry> manifestEntryList) {
        for (ManifestFileMeta manifestFileMeta : manifestFileMetas) {
            List<ManifestEntry> manifestEntries =
                    manifestFileReader.read(manifestFileMeta.fileName());
            manifestEntryList.addAll(manifestEntries);
        }
    }
}
