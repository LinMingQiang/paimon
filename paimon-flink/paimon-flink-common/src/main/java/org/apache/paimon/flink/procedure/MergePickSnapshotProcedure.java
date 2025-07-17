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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.VersionControlOperator;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Cherry-pick snapshot from branch to current branch. */
public class MergePickSnapshotProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "merge_pick";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "from_branch", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "to_branch", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "snapshot_list",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "from_snapshot",
                        type = @DataTypeHint("Integer"),
                        isOptional = true),
                @ArgumentHint(
                        name = "to_snapshot",
                        type = @DataTypeHint("Integer"),
                        isOptional = true),
                @ArgumentHint(
                        name = "overwriteOptions",
                        type = @DataTypeHint("BOOLEAN"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String fromBranchName,
            String toBranchName,
            String snapshotList,
            Integer fromSnapshot,
            Integer toSnapshot,
            Boolean overwriteOptions)
            throws Catalog.TableNotExistException {
        Identifier identifier = Identifier.fromString(tableId);
        FileStoreTable toBranchTable = (FileStoreTable) catalog.getTable(identifier);
        if (!toBranchName.equalsIgnoreCase("main") && !toBranchName.equalsIgnoreCase("master")) {
            toBranchTable = toBranchTable.switchToBranch(toBranchName);
        }
        VersionControlOperator versionControlOperator = toBranchTable.versionControlOperator();
        versionControlOperator.overwriteOptions(overwriteOptions == null || overwriteOptions);
        Snapshot generatedSnapshot;
        if (snapshotList != null && !snapshotList.isEmpty()) {
            generatedSnapshot =
                    versionControlOperator.mergePick(
                            fromBranchName,
                            Arrays.stream(snapshotList.split(","))
                                    .map(Long::parseLong)
                                    .collect(Collectors.toList()));
        } else {
            if (fromSnapshot != null && toSnapshot != null && toSnapshot >= fromSnapshot) {
                generatedSnapshot =
                        versionControlOperator.mergePick(fromBranchName, fromSnapshot, toSnapshot);
            } else {
                throw new IllegalArgumentException(
                        "from_snapshot and to_snapshot must not be null and to_snapshot greater than from_snapshots.");
            }
        }

        return new String[] {
            generatedSnapshot == null
                    ? "Cherry-pick failed."
                    : "Cherry-pick succeeds and generates a new snapshot in the target branch : "
                            + generatedSnapshot.id()
        };
    }
}
