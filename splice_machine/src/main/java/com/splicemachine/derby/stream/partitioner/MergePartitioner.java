package com.splicemachine.derby.stream.partitioner;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
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
                if (BytesUtil.endComparator.compare(result, splits.get(i)) < 0) {
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
