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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for Skipping actions during writing. */
public class WriteSkippingActionsITCase extends CatalogITCaseBase {

    private static final int TIMEOUT = 180;

    @Timeout(value = TIMEOUT)
    @ParameterizedTest
    @EnumSource(CoreOptions.WriteAction.class)
    public void testSkippingCommitActions(CoreOptions.WriteAction skipAction) throws Exception {

        HashMap<String, String> skipActionOptions = createOptions(skipAction.toString());

        createTable("T", skipActionOptions);
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        switch (skipAction) {
            case PARTITION_EXPIRE:
                // Since partition expiration was skipped, there will be one less overwrite type
                // snapshot.
                expectTable(table, snapshotManager, 1, 2, 1, "20250101");
                // Snapshot 2 is COMPACT.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                break;
            case SNAPSHOT_EXPIRE:
                // Test case for skipping snapshot expire.
                // Because snapshot expiration is skipped, all snapshots are retained.
                expectTable(table, snapshotManager, 3, 3, 1, null);
                // Append write.
                assertThat(snapshotManager.snapshot(1).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                // Data compact.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                // Partition expired.
                assertThat(snapshotManager.snapshot(3).commitKind())
                        .isEqualTo(Snapshot.CommitKind.OVERWRITE);
                break;
            case CREATE_TAG:
                // Test case for skipping auto create tag.
                // No tags are generated because the automatic tag creation action is skipped.
                expectTable(table, snapshotManager, 2, 3, 0, null);
                // Partition expired.
                assertThat(snapshotManager.snapshot(3).commitKind())
                        .isEqualTo(Snapshot.CommitKind.OVERWRITE);
        }
    }

    @Timeout(value = TIMEOUT)
    @ParameterizedTest
    @ValueSource(strings = {"do-all", "write-only", "skip-all"})
    public void testSkippingAllActionsAndWriteOnly(String action) throws Exception {

        HashMap<String, String> options =
                createOptions(
                        action.equals("do-all")
                                ? null
                                : "partition-expire,snapshot-expire,create-tag");

        if (action.equals("write-only")) {
            options.put(CoreOptions.WRITE_ONLY.key(), "true");
        }

        createTable("T", options);
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        switch (action) {
            case "do-all":
                // Test case for no actions being skipped. (write-only is false)
                // snapshot count is 2 (snapshot 1 has expired), last snapshot id is 3, auto create
                // tag,
                // partition expired.
                expectTable(table, snapshotManager, 2, 3, 1, null);
                // snapshot 2 is compact, snapshot 3 is overwrite because partition expired.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                assertThat(snapshotManager.snapshot(3).commitKind())
                        .isEqualTo(Snapshot.CommitKind.OVERWRITE);
                break;
            case "write-only":
                // no compact, no expire, no tag.
                expectTable(table, snapshotManager, 1, 1, 0, "20250101");
                assertThat(snapshotManager.latestSnapshot().commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);

                break;
            case "skip-all":
                // All actions are skipped and only the compact is retained.
                // no expire, no tag, only compact.
                expectTable(table, snapshotManager, 2, 2, 0, "20250101");
                // Append write.
                assertThat(snapshotManager.snapshot(1).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                // Data compact.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
        }
    }

    @Test
    @Timeout(value = TIMEOUT)
    public void testSkipCreateTagWithBatchMode() throws Catalog.TableNotExistException {
        HashMap<String, String> options = createOptions("create-tag");

        // Skipping tag creation will not take effect if the tag creation mode is batch.
        options.put(CoreOptions.TAG_AUTOMATIC_CREATION.key(), "batch");

        createTable("T", options);
        sql("INSERT INTO T VALUES ('a', '20250101')");
        FileStoreTable table = paimonTable("T");
        assertThat(table.tagManager().tagCount()).isEqualTo(1);
    }

    private HashMap<String, String> createOptions(String skippingActions) {
        HashMap<String, String> options = new HashMap<>();
        // Partition expiration will be triggered every time.
        options.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "1 d");
        options.put(CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "0 s");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd");
        // Only keep one snapshot.
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        options.put(CoreOptions.TAG_AUTOMATIC_CREATION.key(), "process-time");
        options.put(CoreOptions.TAG_CREATION_PERIOD.key(), "daily");
        // Compact will be triggered every time.
        options.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");

        // skipping actions .
        if (skippingActions != null) {
            options.put(CoreOptions.WRITE_SKIP_ACTIONS.key(), skippingActions);
        }

        return options;
    }

    private void expectTable(
            FileStoreTable table,
            SnapshotManager snapshotManager,
            long snapshotCount,
            long lastSnapshotId,
            long tagCount,
            String partition)
            throws IOException {
        assertThat(snapshotManager.snapshotCount()).isEqualTo(snapshotCount);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(lastSnapshotId);
        assertThat(table.tagManager().tagCount()).isEqualTo(tagCount);
        if (partition == null) {
            assertThat(table.newScan().listPartitions().size()).isEqualTo(0);
        } else {
            assertThat(table.newScan().listPartitions().get(0).getString(0).toString())
                    .isEqualTo(partition);
        }
    }

    private void createTable(String tableName, HashMap<String, String> hintOptions) {

        StringBuilder sb = new StringBuilder();
        sb.append("'bucket' = '1'\n");
        hintOptions.forEach(
                (k, v) -> sb.append(",'").append(k).append("'='").append(v).append("'\n"));

        sql(
                String.format(
                        "CREATE TABLE %s ("
                                + " k STRING,"
                                + " dt STRING,"
                                + " PRIMARY KEY (k, dt) NOT ENFORCED"
                                + ") PARTITIONED BY (dt) WITH ("
                                + "%s"
                                + ")",
                        tableName, sb));
    }
}
