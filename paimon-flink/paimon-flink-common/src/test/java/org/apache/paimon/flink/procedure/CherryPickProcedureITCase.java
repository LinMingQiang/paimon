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
import org.apache.paimon.table.FileStoreTable;
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
    public void testCherryPick() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable mainTable;
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt]");
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

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
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

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
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
        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
        mainTable = paimonTable("T");
        assertThat(mainTable.schema().fields().size()).isEqualTo(4);
        assertThat(mainTable.schema().toSchema().rowType().getField("conflict_col").type())
                .isEqualTo(DataTypes.STRING());

        // 无法转换的将会报错.
        sql("ALTER TABLE `T` ADD (conflict_col2 DATE)");
        sql("ALTER TABLE `T$branch_test` ADD (conflict_col2 INT)");
        sql("INSERT INTO `T$branch_test` VALUES(1, 'branch-apple', 'pt', 'conflict_col_value', 1)");

        assertThatThrownBy(
                        () -> sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 3))
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

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt]");
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
                assertThatThrownBy(
                                () ->
                                        sql(
                                                "CALL sys.cherry_pick('%s', '%s', %s)",
                                                "default.T", "test", 3))
                        .satisfies(
                                anyCauseMatches(
                                        IllegalArgumentException.class,
                                        "Cherry-pick is only supported in APPEND commitKind snapshot."));
            } else {
                assertThatThrownBy(
                                () ->
                                        sql(
                                                "CALL sys.cherry_pick('%s', '%s', %s)",
                                                "default.T", "test", 2))
                        .satisfies(
                                anyCauseMatches(
                                        IllegalArgumentException.class,
                                        "Cherry-pick do not support lookup mode."));
            }

        } else {
            // Do not support dynamic bucket table.
            assertThatThrownBy(
                            () ->
                                    sql(
                                            "CALL sys.cherry_pick('%s', '%s', %s)",
                                            "default.T", "test", 2))
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

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
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
        options.put("bucket", String.valueOf(-1));
        createBranch(false, options);


    }

    public void createBranch(boolean primaryTable, Map<String, String> options) {
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
        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");
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
}
