/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.stream.partitioner;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.IntArrays;
import org.apache.spark.Partitioner;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Created by jleach on 6/9/15.
 */
public class MergePartitioner extends Partitioner implements Externalizable {
        List<byte[]> splits;
        int[] formatIds;
        private transient ThreadLocal<DataHash> encoder = new ThreadLocal<DataHash>() {
            @Override
            protected DataHash initialValue() {
                int[] rowColumns = IntArrays.count(formatIds.length);
                DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(formatIds);
                return BareKeyHash.encoder(rowColumns, null, serializers);
            }
        };

        public MergePartitioner() {

        }

        public MergePartitioner(List<byte[]> splits, int[] formatIds) {
            this.splits = splits;
            this.formatIds = formatIds;
        }

        @Override
        public int numPartitions() {
            return splits.size();
        }

        @Override
        public int getPartition(Object key) {
            ExecRow row = (ExecRow) key;
            DataHash enc = encoder.get();
            enc.setRow(row);
            byte[] result;
            try {
                result = enc.encode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i<splits.size(); ++i) {
                if (Bytes.endComparator.compare(result, splits.get(i)) < 0) {
                    return i;
                }
            }
            return 0;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(splits);
            out.writeObject(formatIds);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            splits = (List<byte[]>) in.readObject();
            formatIds = (int[]) in.readObject();
            encoder = new ThreadLocal<DataHash>() {
                @Override
                protected DataHash initialValue() {
                    int[] rowColumns = IntArrays.count(formatIds.length);
                    DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(formatIds);
                    return BareKeyHash.encoder(rowColumns, null, serializers);
                }
            };
        }
    }
