package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.spark.Partition;
import org.apache.spark.rdd.NewHadoopPartition;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HBasePartitioner extends org.apache.spark.Partitioner implements Partitioner<Object>, Externalizable {
    private boolean[] keyOrder;
    private int[] keyDecodingMap;
    DataSet dataSet;
    transient KeyHashDecoder decoder;
    ExecRow template;
    List<RowPartition> rowPartitions;
    List<TableSplit> tableSplits;

    public HBasePartitioner(DataSet dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder) {
        assert keyDecodingMap !=null:"Key Decoding Map is Null";
        assert template !=null:"Template is Null";
        this.dataSet = dataSet;
        this.template = template;
        this.keyDecodingMap = keyDecodingMap;
        this.keyOrder = keyOrder;
    }

    public HBasePartitioner() {
    }

    @Override
    public void initialize() {
        List<Partition> partitions = ((SparkDataSet) dataSet).rdd.partitions();
        tableSplits = new ArrayList<>(partitions.size());
        for (Partition p : partitions) {
            NewHadoopPartition nhp = (NewHadoopPartition) p;
            SMSplit sms = (SMSplit) nhp.serializableHadoopSplit().value();
            TableSplit ts = sms.getSplit();
            if (ts.getStartRow() != null && Bytes.equals(ts.getStartRow(),ts.getEndRow())) {
                // this would be an empty partition, with the same start and end key, so don't add it
                continue;
            }
            tableSplits.add(ts);
        }
    }

    private ExecRow getRow(byte[] bytesRow) throws StandardException {
        if (bytesRow == null || bytesRow.length == 0) {
            return null;
        }
        decoder.set(bytesRow, 0, bytesRow.length);
        decoder.decode(template);
        return template.getClone();
    }

    @Override
    public int numPartitions() {
        return rowPartitions!=null?rowPartitions.size():tableSplits.size();
    }

    @Override
    public int getPartition(Object o) {
        return getPartition((ExecRow) o);
    }

    public int getPartition(ExecRow row) {
        int result = Collections.binarySearch(rowPartitions, row);
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            assert keyDecodingMap != null : "Key Decoding Map is Null";
            ArrayUtil.writeIntArray(out, keyDecodingMap);
            out.writeBoolean(keyOrder != null);
            if (keyOrder != null)
                ArrayUtil.writeBooleanArray(out, keyOrder);
            ArrayUtil.writeIntArray(out, WriteReadUtils.getExecRowTypeFormatIds(template));
            out.writeInt(tableSplits.size());
            for (TableSplit ts : tableSplits) {
                writeNullableByteArray(out,ts.getStartRow());
                writeNullableByteArray(out,ts.getEndRow());
            }
        } catch (StandardException se) {
            throw new IOException(se);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        try {
            keyDecodingMap = ArrayUtil.readIntArray(in);
            if (in.readBoolean())
                keyOrder = ArrayUtil.readBooleanArray(in);
            template = WriteReadUtils.getExecRowFromTypeFormatIds(ArrayUtil.readIntArray(in));
            int size = in.readInt();
            rowPartitions = new ArrayList<>(size);
            decoder = getDecoder();
            for (int i = 0; i < size; i++) {
                ExecRow start = getRow(readNullableByteArray(in));
                ExecRow end = getRow(readNullableByteArray(in));
                rowPartitions.add(new RowPartition(start, end, template.nColumns()));
            }
        } catch (StandardException se) {
            throw new IOException(se);
        }
    }

    private KeyHashDecoder getDecoder() {
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template.getRowArray());
        DataHash encoder = BareKeyHash.encoder(keyDecodingMap, keyOrder, serializers);
        return encoder.getDecoder();
    }


    private static class RowPartition implements Comparable<ExecRow>, Externalizable {
        ExecRow firstRow;
        ExecRow lastRow;
        int nCols;
        int[] keys;

        public RowPartition(ExecRow firstRow, ExecRow lastRow, int nCols) {
            this.firstRow = firstRow;
            this.lastRow = lastRow;
            this.nCols = nCols;
            initKeys();
        }

        public RowPartition() {
        }

        private void initKeys() {
            keys = new int[nCols];
            for (int i = 0; i<nCols; ++i) {
                keys[i] = i + 1;
            }
        }

        @Override
        public int compareTo(ExecRow o) {
            int comparison;
            if (lastRow != null) {
                comparison = lastRow.compareTo(keys, o);
                if (comparison <= 0) return comparison;
            }
            if (firstRow != null) {
                comparison = firstRow.compareTo(keys, o);
                if (comparison >= 0) return 1;
            }
            return 0;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(firstRow);
            out.writeObject(lastRow);
            out.writeInt(nCols);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            firstRow = (ExecRow) in.readObject();
            lastRow = (ExecRow) in.readObject();
            nCols = in.readInt();
            initKeys();
        }
    }

    private static void writeNullableByteArray(ObjectOutput out, byte[] value) throws IOException {
        out.writeBoolean(value!=null);
        if (value!=null) {
            out.writeInt(value.length);
            out.write(value);
        }
    }
    private static byte[] readNullableByteArray(ObjectInput in) throws IOException {
        if (!in.readBoolean())
            return null;
        byte[] value = new byte[in.readInt()];
        in.readFully(value);
        return value;
    }

}