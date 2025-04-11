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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/** Cherry-pick snapshot from branch to current branch. */
public class CherryPickSnapshotProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "cherry_pick";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "from_branch", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "to_branch", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "snapshot", type = @DataTypeHint("Integer")),
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
            Integer snapshot,
            Boolean overwriteOptions)
            throws Catalog.TableNotExistException {
        Identifier identifier = Identifier.fromString(tableId);
        FileStoreTable toBranchTable = (FileStoreTable) catalog.getTable(identifier);
        if (!toBranchName.equalsIgnoreCase("main") && !toBranchName.equalsIgnoreCase("master")) {
            toBranchTable = toBranchTable.switchToBranch(toBranchName);
        }
        Snapshot generatedSnapshot =
                toBranchTable
                        .versionControlOperator()
                        .overwriteOptions(overwriteOptions == null || overwriteOptions)
                        .cherryPick(fromBranchName, snapshot);
        return new String[] {
            generatedSnapshot == null
                    ? "Cherry-pick failed."
                    : "Cherry-pick succeeds and generates a new snapshot in the target branch : "
                            + generatedSnapshot.id()
        };
    }
}
