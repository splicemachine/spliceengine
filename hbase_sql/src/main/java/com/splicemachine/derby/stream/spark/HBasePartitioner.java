/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
import java.util.Arrays;
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
    private int[] rightHashKeys;

    public HBasePartitioner(DataSet dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        assert keyDecodingMap !=null:"Key Decoding Map is Null";
        assert template !=null:"Template is Null";
        this.dataSet = dataSet;
        this.template = template;
        this.keyDecodingMap = keyDecodingMap;
        this.keyOrder = keyOrder;
        this.rightHashKeys = rightHashKeys;
    }

    public HBasePartitioner() {
    }

    @Override
    public void initialize() {
        List<Partition> partitions = Arrays.asList(((SparkDataSet) dataSet).rdd.rdd().partitions());
        tableSplits = new ArrayList<>(partitions.size());
        for (Partition p : partitions) {
            NewHadoopPartition nhp = (NewHadoopPartition) p;
            SMSplit sms = (SMSplit) nhp.serializableHadoopSplit().value();
            TableSplit ts = sms.getSplit();
            if (ts.getStartRow() != null && Bytes.equals(ts.getStartRow(),ts.getEndRow()) && ts.getStartRow().length > 0) {
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
        // Decode to template and then move towards a key based comparison
        ExecRow hashRow = new ValueRow(rightHashKeys.length);
        for (int i =0;i<rightHashKeys.length;i++) {
            hashRow.setColumn(i+1,template.cloneColumn(rightHashKeys[i]+1));
        }
        return hashRow;
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
        if (result < 0)
            return 0;
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            assert keyDecodingMap != null : "Key Decoding Map is Null";
            assert rightHashKeys != null : "Right Hash Keys are Null";
            ArrayUtil.writeIntArray(out, keyDecodingMap);
            out.writeBoolean(keyOrder != null);
            if (keyOrder != null)
                ArrayUtil.writeBooleanArray(out, keyOrder);
            ArrayUtil.writeIntArray(out, WriteReadUtils.getExecRowTypeFormatIds(template));
            ArrayUtil.writeIntArray(out,rightHashKeys);
            out.writeInt(tableSplits.size());
            for (TableSplit ts : tableSplits) {
                writeNullableByteArray(out,ts.getStartRow());
                writeNullableByteArray(out,ts.getEndRow());
            }
        } catch (Exception se) {
            throw new IOException(se);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        try {
            keyDecodingMap = ArrayUtil.readIntArray(in);
            if (in.readBoolean())
                keyOrder = ArrayUtil.readBooleanArray(in);
            template = WriteReadUtils.getExecRowFromTypeFormatIds(ArrayUtil.readIntArray(in));
            rightHashKeys = ArrayUtil.readIntArray(in);
            int size = in.readInt();
            rowPartitions = new ArrayList<>(size);
            decoder = getDecoder();
            for (int i = 0; i < size; i++) {
                ExecRow start = getRow(readNullableByteArray(in));
                ExecRow end = getRow(readNullableByteArray(in));
                rowPartitions.add(new RowPartition(start, end));
            }
        } catch (Exception se) {
            throw new IOException(se);
        }
    }

    private KeyHashDecoder getDecoder() {
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template.getRowArray());
        DataHash encoder = BareKeyHash.encoder(keyDecodingMap, keyOrder, serializers);
        return encoder.getDecoder();
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
