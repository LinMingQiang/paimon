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

package org.apache.paimon.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.util.Arrays;

/** PartitionValuesTimeExpireStrategy. */
public class PartitionValuesTimeExpireStrategy extends PartitionExpireStrategy {

    private final PartitionTimeExtractor timeExtractor;

    public PartitionValuesTimeExpireStrategy(CoreOptions options, RowType partitionType) {
        super(options, partitionType);
        String timePattern = options.partitionTimestampPattern();
        String timeFormatter = options.partitionTimestampFormatter();
        this.timeExtractor = new PartitionTimeExtractor(timePattern, timeFormatter);
    }

    @Override
    public PartitionPredicate createPartitionPredicate(LocalDateTime expirationTime) {
        return new PartitionValuesTimePredicate(expirationTime);
    }

    private class PartitionValuesTimePredicate implements PartitionPredicate {

        private final LocalDateTime expireDateTime;

        private PartitionValuesTimePredicate(LocalDateTime expireDateTime) {
            this.expireDateTime = expireDateTime;
        }

        @Override
        public boolean test(BinaryRow partition) {
            Object[] array = toObjectArrayConverter.convert(partition);
            LocalDateTime partTime = timeExtractor.extract(partitionKeys, Arrays.asList(array));
            return partTime != null && expireDateTime.isAfter(partTime);
        }

        @Override
        public boolean test(BinaryRow partition, Timestamp creationTime) {
            return test(partition);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            return true;
        }
    }
}
