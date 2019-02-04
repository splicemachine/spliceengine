/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.io.hbase.encoding;

import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.art.SimpleART;
import com.splicemachine.art.node.Base;
import com.splicemachine.art.tree.ART;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLTinyint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.HCell;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.EncodingState;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.hive.orc.OrcSerializer;
import org.apache.spark.sql.types.StructType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.NONE;


/**
 *
 * Encoding State that stores the per thread information from flushes.
 *
 *
 */
public class PAXEncodingState extends EncodingState {
    private static final byte FIXED_INT64 = 0x2c;
    private static final byte MASK = (byte) 0xff;
    private static Logger LOG = Logger.getLogger(PAXEncodingState.class);
    private ART radixTree;
    //private ART radixTree2;  // msirek-temp
    private ExecRow execRow;
    private ExecRow writtenExecRow;
    private List<? extends StructField> structFields;
    private Writer writer;
    private OrcSerializer orcSerializer;
    private OrcStruct orcStruct;
    private SettableStructObjectInspector oi;
    private StructType type;
    private int counter;
    private List<String> items;
    private static Path DUMMY_PATH = new Path("/");
    private UnsafeRecord unsafeRecord;
    private HCell hcell;
    private int[] dataToRetrieve;
    private long beginMillis;

    private static class Entry<K, V> {
        public K key;
        public V value;
        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
    private ArrayList<Entry<byte[], byte[]>> kvList = null;


    public PAXEncodingState(DataOutputStream out) throws IOException {
        beginMillis = System.currentTimeMillis();
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "init PAXEncodingState on thread={%s}", Thread.currentThread().getName());
        }
        if (LOG.isTraceEnabled()) {
            items = new ArrayList<>();
        }
        unsafeRecord = new UnsafeRecord();
        hcell = new HCell();
        if (execRow == null) {
            init(ConglomerateUtils.conglomerateThreadLocal.get()); // Can Null Point
        }
        dataToRetrieve = IntArrays.count(execRow.size());
        radixTree = new SimpleART();
        //radixTree2 = new SimpleART();
        type = writtenExecRow.createStructType(IntArrays.count(writtenExecRow.size())); // Can we overload this method on execrow...
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"attempting to encode types=%s",type.catalogString());
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type.catalogString());
        oi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        orcSerializer = new OrcSerializer(type,new Configuration());
        orcStruct = (OrcStruct) oi.create();
        writer = OrcFile.createWriter(new PAXBlockFileSystem(out),DUMMY_PATH, new Configuration(),oi,268435456, NONE, 262144, 10000);
        structFields = oi.getAllStructFieldRefs();
        counter = 0;

        kvList = new ArrayList<>();
    }

    /**
     *
     * From the ExecRow supplied, create the WritableRow representing the ORC structure.
     *
     * @param execRow
     * @return
     * @throws StandardException
     */
    public static ExecRow createWritableRow(ExecRow execRow) throws StandardException {
        ExecRow writableRow =  new ValueRow(execRow.size() + 6);
        writableRow.setColumn(1, new SQLLongint()); // Timestamp
        writableRow.setColumn(2, new SQLTinyint()); // Byte
        writableRow.setColumn(3, new SQLLongint()); // Sequence ID
        writableRow.setColumn(4, new SQLBoolean()); // Tombstone
        writableRow.setColumn(5, new SQLLongint()); // TxnID
        writableRow.setColumn(6, new SQLLongint()); // Effective Timestamp
        for (int i = 1; i <= execRow.size(); i++) {
            writableRow.setColumn(6 + i, execRow.getColumn(i));
        }
        return writableRow;
    }

    private void init(ExecRow execRow) {
        try {
            if (execRow == null)
                throw new UnsupportedOperationException("SpliceQuery was not passed on the scan or get method.  Block reading requires Splice Query.");
            this.execRow = execRow;
            this.writtenExecRow = createWritableRow(execRow);
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    public static byte[] genRowKey(Cell cell) throws IOException {
        short rowLen = cell.getRowLength();
        byte[] values = new byte[rowLen+1];
        Base.U.copyMemory(cell.getRowArray(),cell.getRowOffset()+16,values,16,cell.getRowLength());
        values[rowLen] = 0x00; // Attempt to not have overlapping keys?
        return values;
    }

    public int encode(Cell cell) throws IOException {
        try {
            // msirek-temp->
                if (cell instanceof KeyValue &&
                        ((KeyValue) cell).getLength() < 40)
                    return 0;
            // <- msirek-temp
            byte[] valueBytes = new byte[12];
            Base.U.putLong(valueBytes,16l,cell.getTimestamp());
            Base.U.putInt(valueBytes,24l,counter);
            byte[] rowKey = genRowKey(cell);
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.trace(LOG,"key to insert into radix key=%s, timestamp=%d",
                        Bytes.toHex(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),cell.getTimestamp());
                items.add(Bytes.toHex(rowKey, 0, rowKey.length));
            }
            kvList.add(new Entry<>(rowKey, valueBytes));
            //radixTree.insert(rowKey, 0, rowKey.length, valueBytes, 0, valueBytes.length);
            counter++;
           // if ((counter % 10000) == 0)
           //     radixTree.checkTree(); // msirek-temp
            //SpliceLogUtils.info(LOG, "ART for row #%d : \n%s \n",counter, radixTree.toString());  // msirek-temp
            //LOG.trace("ART for row #%d : \n%s \n",counter, radixTree.toString());  // msirek-temp
            hcell.set(cell);
            unsafeRecord.wrap(hcell);
            unsafeRecord.getData(dataToRetrieve, execRow);
            writtenExecRow.getColumn(1).setValue(cell.getTimestamp()); // Version
            writtenExecRow.getColumn(2).setValue(cell.getTypeByte()); // Type
            writtenExecRow.getColumn(3).setValue(cell.getSequenceId());// Sequence ID
            writtenExecRow.getColumn(4).setValue(unsafeRecord.hasTombstone()); // Tombstone
            writtenExecRow.getColumn(5).setValue(unsafeRecord.getTxnId1()); // TxnID
            writtenExecRow.getColumn(6).setValue(unsafeRecord.getEffectiveTimestamp());// Effective Timestamp
            for (int i = 0; i < structFields.size(); i++) {
                oi.setStructFieldData(orcStruct, structFields.get(i),
                        writtenExecRow.getColumn(i + 1).getHiveObject());
            }
            writer.addRow(orcStruct);
            return 1;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void endBlockEncoding(DataOutputStream out, byte[] uncompressedBytesWithHeader) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "endBlockEncoding on thread={%s}", Thread.currentThread().getName());
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "[Begin] Keys in Radix Tree");
            for (String item : items) {
                SpliceLogUtils.trace(LOG,"  %s",item);
            }
            SpliceLogUtils.trace(LOG, "[End] Keys in Radix Tree");
            SpliceLogUtils.trace(LOG,"Radix Tree:\n%s",radixTree.debugString(false));
        }
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        //DataOutputStream daos = new DataOutputStream(baos);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Begin Serialize Tree on thread={%s}",Thread.currentThread().getName());
        int i = 0;
        for (Entry<byte[], byte[]> kv : kvList) {
            i++;
            //if (i <= 10)
                radixTree.insert(kv.key, 0, kv.key.length, kv.value, 0, kv.value.length);
            //else
               // radixTree2.insert(kv.key, 0, kv.key.length, kv.value, 0, kv.value.length);
        }
        //radixTree.serialize(daos);  msirek-temp
        //daos.flush();
        radixTree.serialize(out);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"End Serialize Tree on thread={%s}, bytes=%d",Thread.currentThread().getName(),out.size());
        //out.writeInt(daos.size());
        //byte[] foo = baos.toByteArray();
        //out.write(foo,0,foo.length);
        writer.close();
        LOG.error(String.format("End Block Encoding with rows=%d, time(ms)=%d, treeSize(bytes)=%d, columnarSize(bytes)=%d, format=%s",counter, System.currentTimeMillis()-beginMillis,out.size(), out.size(), writtenExecRow));
        //daos.close();
        radixTree.destroy(); // Critical, will leak memory if you do not do this...
        radixTree = null;
        //radixTree2.destroy(); // Critical, will leak memory if you do not do this...
        //radixTree2 = null;
        unsafeRecord = null;
        hcell = null;
        oi = null;
        orcSerializer = null;
        orcStruct = null;
        writer = null;
        structFields = null;
        kvList = null;
    }

}
