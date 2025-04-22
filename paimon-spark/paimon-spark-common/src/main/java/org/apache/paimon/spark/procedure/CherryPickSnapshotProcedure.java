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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Cherry-pick snapshot from branch to current branch. */
public class CherryPickSnapshotProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("branch", StringType),
                ProcedureParameter.required("snapshot", IntegerType),
                ProcedureParameter.optional("overwriteOptions", BooleanType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", BooleanType, true, Metadata.empty())
                    });

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    protected CherryPickSnapshotProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String branchName = args.isNullAt(1) ? null : args.getString(1);
        Integer snapshot = args.isNullAt(2) ? null : args.getInt(2);
        Boolean overwriteOptions = args.isNullAt(3) ? null : args.getBoolean(3);

        FileStoreTable mainTable = (FileStoreTable) loadSparkTable(tableIdent).getTable();
        Snapshot updatedSnapshot =
                mainTable
                        .versionControlOperator()
                        .overwriteOptions(overwriteOptions == null || overwriteOptions)
                        .cherryPick(branchName, snapshot);
        return new InternalRow[] {
            newInternalRow("Cherry-pick to snapshotID : " + updatedSnapshot.id())
        };
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CherryPickSnapshotProcedure>() {
            @Override
            public CherryPickSnapshotProcedure doBuild() {
                return new CherryPickSnapshotProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CherryPickSnapshotProcedure";
    }
}
