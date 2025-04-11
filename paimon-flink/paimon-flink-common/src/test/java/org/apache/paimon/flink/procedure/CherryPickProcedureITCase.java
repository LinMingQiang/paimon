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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link CherryPickSnapshotProcedure }. */
public class CherryPickProcedureITCase extends CatalogITCaseBase {

    @Override
    @BeforeEach
    public void before() throws IOException {
        options.put("cache-enabled", "false");
        super.before();
    }

    @Test
    public void testCherryPickWithAddCol() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT"
                        + ", v STRING"
                        + ", pt STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '-1'"
                        + ",'write-only' = 'true' \n"
                        + ",'changelog-producer' = 'input' \n"
                        + ",'file.format' = 'parquet' \n"
                        + " )");

        sql("INSERT INTO T VALUES" + " (1, 'apple', 'pt')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");

        FileStoreTable mainTable = paimonTable("T");
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        // Add v2 column for branch table.
        sql("ALTER TABLE `T$branch_test` ADD (v2 STRING)");
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt', 'v2')");

        assertThat(collectResult("SELECT * FROM `T$branch_test`"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt, v2]");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        assertThat(branchTable.schema().fields().size()).isEqualTo(4);

        sql("INSERT INTO T VALUES" + " (1, 'main-apple', 'pt')");

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 2);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(3);
        assertThat(mainTable.schema().fields().size()).isEqualTo(4);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, main-apple, pt, null]");
    }

    @Test
    public void testCherryPickWithSchemaMerge() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT"
                        + ", v STRING"
                        + ", pt STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '-1'"
                        + ",'write-only' = 'true' \n"
                        + ",'changelog-producer' = 'input' \n"
                        + ",'file.format' = 'parquet' \n"
                        + " )");

        sql("INSERT INTO T VALUES" + " (1, 'apple', 'pt')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");

        FileStoreTable mainTable = paimonTable("T");
        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(1);

        // Add v2 column for branch table.
        sql("ALTER TABLE `T$branch_test` ADD (v2 STRING)");
        sql("INSERT INTO `T$branch_test` VALUES " + "(1, 'branch-apple', 'pt', 'v2')");

        assertThat(collectResult("SELECT * FROM `T$branch_test`"))
                .containsExactlyInAnyOrder("+I[1, branch-apple, pt, v2]");
        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(2);
        assertThat(branchTable.schema().fields().size()).isEqualTo(4);

        sql("INSERT INTO T VALUES" + " (1, 'main-apple', 'pt')");
        sql("ALTER TABLE `T` ADD (v3 STRING)");
        sql("INSERT INTO T VALUES" + " (1, 'main-new-apple', 'pt', 'v3')");

        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(3);
        assertThat(mainTable.schema().fields().size()).isEqualTo(4);

        sql("INSERT INTO `T$branch_test` VALUES " + "(2, 'new-branch-apple', 'pt', 'v2')");

        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isEqualTo(3);

        sql("CALL sys.cherry_pick('%s', '%s', %s)", "default.T", "test", 3);
        mainTable = paimonTable("T");
        assertThat(mainTable.snapshotManager().latestSnapshotId()).isEqualTo(4);
        assertThat(mainTable.schema().fields().size()).isEqualTo(5);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, main-new-apple, pt, v3, null]",
                        "+I[2, new-branch-apple, pt, null, v2]");
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
