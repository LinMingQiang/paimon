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

package org.apache.paimon.table.system;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.BucketSpec;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing summary of the specific table. */
public class SummaryTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String SUMMARY = "summary";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "table_name", SerializationUtils.newStringType(true)),
                            new DataField(
                                    0, "table_type", SerializationUtils.newStringType(true))));

    private final FileStoreTable storeTable;

    public SummaryTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public InnerTableScan newScan() {
        return new SummaryScan(storeTable);
    }

    @Override
    public InnerTableRead newRead() {
        return new SummaryRead(storeTable);
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + SUMMARY;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("table_name");
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SummaryTable(storeTable.copy(dynamicOptions));
    }

    private static class SummaryScan extends ReadOnceTableScan {

        private final FileStoreTable fileStoreTable;

        private SummaryScan(FileStoreTable fileStoreTable) {
            this.fileStoreTable = fileStoreTable;
        }

        @Override
        protected Plan innerPlan() {
            return () -> Collections.singletonList(new SummarySplit());
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }
    }

    private static class SummarySplit extends SingletonSplit {}

    private static class SummaryRead implements InnerTableRead {

        private RowType readType;

        private final FileStoreTable storeTable;

        private SummaryRead(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }


        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            Preconditions.checkArgument(
                    split instanceof SummarySplit, "Unsupported split: " + split.getClass());

            String tableName = storeTable.fullName();
            storeTable.location();
            BucketSpec bs = storeTable.bucketSpec();
            Optional<String> comment = storeTable.comment();
            List<String> part = storeTable.partitionKeys();
            List<String> pk = storeTable.primaryKeys();
            Map<String, String> options = storeTable.options();
            long schemaId = storeTable.schema().id();
            int tagNums = storeTable.tagManager().allTagNames().size();
            Long li = storeTable.snapshotManager().latestSnapshotId();
            Long ei = storeTable.snapshotManager().earliestSnapshotId();
            String s = storeTable.snapshotManager().snapshot(1L).toJson();

            SnapshotReader snr = storeTable.newSnapshotReader();
            List<BucketEntry> c = snr.bucketEntries();
            List<PartitionEntry> pe = snr.partitionEntries();
            ManifestsReader.Result r = snr.manifestsReader().read(storeTable.snapshotManager().earliestSnapshot(), ScanMode.ALL);


            return new IteratorRecordReader<>(Collections.singletonList(toRow()).iterator());
        }

        public InternalRow toRow() {
            return GenericRow.of(BinaryString.fromString("hello"),
                    BinaryString.fromString("append"));
        }
    }
}
