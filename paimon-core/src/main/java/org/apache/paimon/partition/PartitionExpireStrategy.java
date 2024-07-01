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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** PartitionExpireStrategy. */
public abstract class PartitionExpireStrategy {

    public List<String> partitionKeys;
    public CoreOptions options;
    public RowDataToObjectArrayConverter toObjectArrayConverter;

    public PartitionExpireStrategy(CoreOptions options, RowType partitionType) {
        this.options = options;
        this.toObjectArrayConverter = new RowDataToObjectArrayConverter(partitionType);
        this.partitionKeys = partitionType.getFieldNames();
    }

    public Map<String, String> toPartitionString(Object[] array) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            map.put(partitionKeys.get(i), array[i].toString());
        }
        return map;
    }

    public Object[] convertPartition(BinaryRow partition) {
        return toObjectArrayConverter.convert(partition);
    }

    public abstract PartitionPredicate createPartitionPredicate(LocalDateTime expirationTime);

    public static PartitionExpireStrategy createPartitionPredicate(
            CoreOptions options, RowType partitionType) {
        switch (options.partitionExpireStrategy()) {
            case PARTITION_UPDATE_TIME:
                return new PartitionUpdateTimeExpireStrategy(options, partitionType);
            case PARTITION_VALUE:
            default:
                return new PartitionValuesTimeExpireStrategy(options, partitionType);
        }
    }
}
