/*
 * Copyright 2012 - 2020 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.access.client;

import org.spark_project.guava.collect.Ordering;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Created by dgomezferro on 2/22/17.
 */
public class MemstoreKeyValueScannerTest {
    @Test
    public void testExistingResultsAreOrdered() throws IOException {
        byte[] row = Bytes.toBytes("first");

        ResultScanner rs = generateResultScanner(generateKV(row, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES, Bytes.toBytes("delete")));

        MemstoreKeyValueScanner mkvs = new MemstoreKeyValueScanner(rs);

        List<Cell> results = new ArrayList<>();
        results.add(generateKV(row, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, Bytes.toBytes("commit")));
        results.add(generateKV(row, SIConstants.PACKED_COLUMN_BYTES, Bytes.toBytes("value")));

        mkvs.next(results);

        assertEquals("Number of results doesn't match", 3, results.size());
        assertTrue("Results are not ordered", Ordering.from(KeyValue.COMPARATOR).isOrdered(results));
    }

    private KeyValue generateKV(byte[] row, byte[] qualifier, byte[] value) {
        byte[] cf = Bytes.toBytes("V");
        return new KeyValue(row, cf, qualifier, 10, value);
    }

    private ResultScanner generateResultScanner(KeyValue... kvs) {
        TreeSet<KeyValue> set = new TreeSet<>(KeyValue.COMPARATOR);

        set.addAll(Arrays.asList(kvs));

        KeyValue[] sortedKvs = new KeyValue[set.size()];
        set.toArray(sortedKvs);

        final Result result = Result.create(kvs);

        return new ResultScanner() {
            @Override
            public Result next() throws IOException {
                return result;
            }

            @Override
            public Result[] next(int nbRows) throws IOException {
                return new Result[] {result};
            }

            @Override
            public void close() {

            }

            @Override
            public Iterator<Result> iterator() {
                return Arrays.asList(result).iterator();
            }
        };
    }

}
