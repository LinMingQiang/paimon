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

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link MergePickSnapshotProcedure }. */
public class MergePickProcedureITCase extends CatalogITCaseBase {

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
    public void testMergePick() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        sql("INSERT INTO `T$branch_test` VALUES " + "(2, 'branch2-apple2', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        // pick snapshot 2.
        mergePick("default.T", "test", "main", "2,3", false);
        FileStoreTable mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt]", "+I[2, branch2-apple2, pt]");
    }

    @Test
    public void testMergePickUseRangePick() throws Exception {
        createBranch(true, getCoreOptions());
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt')");
        sql("INSERT INTO `T$branch_test` VALUES " + "(2, 'branch2-apple2', 'pt')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        // pick snapshot 2.
        mergePick("default.T", "test", "main", 2, 3, false);
        FileStoreTable mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt]", "+I[2, branch2-apple2, pt]");
    }

    @Test
    public void testMergePickMergeSchemas() throws Exception {
        createBranch(true, getCoreOptions());

        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);
        sql("ALTER TABLE `T$branch_test` ADD (v2 STRING)");
        sql("INSERT INTO `T$branch_test` VALUES(1, 'branch-apple', 'pt', 'v2')");

        sql("ALTER TABLE `T$branch_test` ADD (v3 STRING)");
        sql("INSERT INTO `T$branch_test` VALUES(2, 'branch2-apple2', 'pt', 'v2', 'v3')");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        // pick snapshot 2.
        mergePick("default.T", "test", "main", "2,3", false);
        FileStoreTable mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(2);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, branch-apple, pt, v2, null]", "+I[2, branch2-apple2, pt, v2, v3]");
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

    private void mergePick(
            String tableId,
            String fromBranchName,
            String toBranchName,
            String snapshotList,
            Boolean overwriteOptions) {
        sql(
                "CALL sys.merge_pick(`table` => '%s',"
                        + "`from_branch` =>'%s', "
                        + "`to_branch` =>'%s', "
                        + "`snapshot_list` => '%s',"
                        + "`overwriteOptions` => %s)",
                tableId, fromBranchName, toBranchName, snapshotList, overwriteOptions);
    }

    private void mergePick(
            String tableId,
            String fromBranchName,
            String toBranchName,
            int fromSnapshot,
            int toSnapshot,
            Boolean overwriteOptions) {
        sql(
                "CALL sys.merge_pick(`table` => '%s',"
                        + "`from_branch` =>'%s', "
                        + "`to_branch` =>'%s', "
                        + "`from_snapshot` => %s,"
                        + "`to_snapshot` => %s,"
                        + "`overwriteOptions` => %s)",
                tableId, fromBranchName, toBranchName, fromSnapshot, toSnapshot, overwriteOptions);
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
