package com.splicemachine.derby.stream.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.serialization.ActivationSerializer;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.example.ExecRowUtils;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.DataInputBuffer;
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
    List<RowPartition> rowPartitions = new ArrayList<>();

    public HBasePartitioner(DataSet dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder) {
        this.dataSet = dataSet;
        this.template = template;
        this.keyDecodingMap = keyDecodingMap;
        this.keyOrder = keyOrder;
        initDecoder();
    }

    public HBasePartitioner() {
    }

    @Override
    public void initialize() {
        List<Partition> partitions = ((SparkDataSet) dataSet).rdd.partitions();
        for (Partition p : partitions) {
            NewHadoopPartition nhp = (NewHadoopPartition) p;
            SMSplit sms = (SMSplit) nhp.serializableHadoopSplit().value();
            TableSplit ts = sms.getSplit();
            try {
                rowPartitions.add(new RowPartition(getRow(ts.getStartRow()), getRow(ts.getEndRow()), template.nColumns()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ExecRow getRow(byte[] bytesRow) throws StandardException {
        if (bytesRow == null || bytesRow.length == 0) {
            return null;
        }
        decoder.set(bytesRow, 0, bytesRow.length);
        decoder.decode(template);
        return trim(template);
    }

    private ExecRow trim(ExecRow row) throws StandardException{
        int ncols = 0;
        while (row.getColumn(ncols+1) != null && !row.getColumn(ncols+1).isNull())
            ncols++;

        ValueRow result = new ValueRow(ncols);
        for (int i = 0; i < ncols; ++i) {
            result.setColumn(i+1, row.getColumn(i+1).cloneValue(false));
        }
        return result;
    }

    @Override
    public int numPartitions() {
        return rowPartitions.size();
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
        out.writeObject(keyDecodingMap);
        out.writeObject(keyOrder);
        out.writeObject(rowPartitions);
        KryoPool kryoPool=SpliceKryoRegistry.getInstance();
        Kryo kryo=kryoPool.get();
        Output output=new Output(4096,-1);
        try{
            kryo.writeClassAndObject(output, template);
        }finally{
            output.flush();
            kryoPool.returnInstance(kryo);
        }
        byte[] data = output.toBytes();
        out.writeInt(data.length);
        out.write(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        keyDecodingMap = (int[]) in.readObject();
        keyOrder = (boolean[]) in.readObject();
        rowPartitions = (List<RowPartition>) in.readObject();
        int size = in.readInt();
        byte[] data = new byte[size];
        in.readFully(data);
        KryoPool kryoPool= SpliceKryoRegistry.getInstance();
        Kryo kryo=kryoPool.get();
        Input input = new Input(data);
        try{
            template = (ExecRow) kryo.readClassAndObject(input);
        } finally {
            kryoPool.returnInstance(kryo);
            input.close();
        }
        initDecoder();
    }

    private void initDecoder() {
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template.getRowArray());
        DataHash encoder = BareKeyHash.encoder(keyDecodingMap, keyOrder, serializers);
        this.decoder = encoder.getDecoder();
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
}