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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.IntArrays;
import org.apache.commons.collections.iterators.EmptyListIterator;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RowAndIndexGenerator extends SpliceFlatMapFunction<SpliceBaseOperation, LocatedRow, Tuple2<Long, Tuple2<byte[], byte[]>>> {
    private static final long serialVersionUID = 844136943916989111L;
    protected int[] pkCols;
    protected String tableVersion;
    protected ExecRow execRowDefinition;
    protected RowLocation[] autoIncrementRowLocationArray;
    protected KVPair.Type dataType;
    protected SpliceSequence[] spliceSequences;
    protected PairEncoder encoder;
    protected InsertOperation insertOperation;
    protected byte[] destinationTable;
    protected boolean initialized;
    protected TxnView txn;
    protected long heapConglom;
    protected ArrayList<DDLMessage.TentativeIndex> tentativeIndices;
    protected IndexTransformFunction[] indexTransformFunctions;

    public RowAndIndexGenerator() {

    }

    public RowAndIndexGenerator(int[] pkCols,
                          String tableVersion,
                          ExecRow execRowDefinition,
                          RowLocation[] autoIncrementRowLocationArray,
                          SpliceSequence[] spliceSequences,
                          long heapConglom,
                          TxnView txn,
                          OperationContext operationContext,
                          ArrayList<DDLMessage.TentativeIndex> tentativeIndices) {
        super(operationContext);
        assert txn !=null:"txn not supplied";
        this.txn = txn;
        this.pkCols = pkCols;
        this.tableVersion = tableVersion;
        this.execRowDefinition = execRowDefinition;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.spliceSequences = spliceSequences;
        this.destinationTable = Bytes.toBytes(Long.toString(heapConglom));
        this.heapConglom = heapConglom;
        this.tentativeIndices = tentativeIndices;
        if (operationContext!=null) {
            this.insertOperation = (InsertOperation) operationContext.getOperation();
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            out.writeLong(heapConglom);
            out.writeBoolean(operationContext!=null);
            if (operationContext!=null)
                out.writeObject(operationContext);
            SIDriver.driver().getOperationFactory().writeTxn(txn, out);
            ArrayUtil.writeIntArray(out, pkCols);
            out.writeUTF(tableVersion);
            out.writeObject(execRowDefinition);
            out.writeInt(autoIncrementRowLocationArray.length);
            for (int i = 0; i < autoIncrementRowLocationArray.length; i++)
                out.writeObject(autoIncrementRowLocationArray[i]);
            out.writeInt(spliceSequences.length);
            for (int i =0; i< spliceSequences.length; i++) {
                out.writeObject(spliceSequences[i]);
            }
            out.writeLong(heapConglom);
            out.writeInt(tentativeIndices.size());
            for (DDLMessage.TentativeIndex ti: tentativeIndices) {
                byte[] message = ti.toByteArray();
                out.writeInt(message.length);
                out.write(message);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        heapConglom = in.readLong();
        if (in.readBoolean())
            operationContext = (OperationContext) in.readObject();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        pkCols = ArrayUtil.readIntArray(in);
        tableVersion = in.readUTF();
        execRowDefinition = (ExecRow) in.readObject();
        autoIncrementRowLocationArray = new RowLocation[in.readInt()];
        for (int i = 0; i < autoIncrementRowLocationArray.length; i++)
            autoIncrementRowLocationArray[i] = (RowLocation) in.readObject();
        spliceSequences = new SpliceSequence[in.readInt()];
        for (int i =0; i< spliceSequences.length; i++)
            spliceSequences[i] = (SpliceSequence) in.readObject();
        heapConglom = in.readLong();
        int iSize = in.readInt();
        tentativeIndices = new ArrayList<>(iSize);
        for (int i = 0; i< iSize; i++) {
            byte[] message = new byte[in.readInt()];
            in.readFully(message);
            tentativeIndices.add(DDLMessage.TentativeIndex.parseFrom(message));
        }
    }

    @Override
    public Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>> call(LocatedRow locatedRow) throws Exception {
        ExecRow execRow = locatedRow.getRow();
        if (!initialized) {
            encoder = new PairEncoder(getKeyEncoder(), getRowHash(), dataType);
            int i = 0;
            indexTransformFunctions = new IndexTransformFunction[tentativeIndices.size()];
            for (DDLMessage.TentativeIndex index: tentativeIndices) {
                indexTransformFunctions[i] = new IndexTransformFunction(index);
                i++;
            }
            initialized = true;
        }

        try {
            ArrayList<Tuple2<Long,Tuple2<byte[], byte[]>>> list = new ArrayList();
            KVPair mainRow = encoder.encode(execRow);
            locatedRow.setRowLocation(new HBaseRowLocation(mainRow.rowKeySlice()));
            list.add(new Tuple2<>(heapConglom,new Tuple2<>(mainRow.getRowKey(), mainRow.getValue())));
            for (int i = 0; i< indexTransformFunctions.length; i++) {
                LocatedRow indexRow = getIndexRow(indexTransformFunctions[i], locatedRow);
                Long indexConglomerate = indexTransformFunctions[i].getIndexConglomerateId();
                KVPair indexKVPair = indexTransformFunctions[i].call(indexRow);
                list.add(new Tuple2<>(indexConglomerate, new Tuple2<>(indexKVPair.getRowKey(), indexKVPair.getValue())));
            }
            return list.iterator();

        } catch (Exception e) {
            if (operationContext!=null && operationContext.isPermissive()) {
                operationContext.recordBadRecord(e.getLocalizedMessage() + execRow.toString(), e);
                return EmptyListIterator.INSTANCE;
            }
            throw Exceptions.parseException(e);
        }
    }

    /**
     * Strip off all non-index columns from a main table row
     */
    private LocatedRow getIndexRow(IndexTransformFunction indexTransformFunction, LocatedRow locatedRow) throws StandardException{
        ExecRow execRow = locatedRow.getRow();
        List<Integer> indexColToMainCol = indexTransformFunction.getIndexColsToMainColMapList();
        List<Integer> sortedList = new ArrayList<>(indexColToMainCol);
        Collections.sort(sortedList);
        ExecRow row = new ValueRow(indexColToMainCol.size());
        int col = 1;
        for (Integer n : sortedList) {
            row.setColumn(col, execRow.getColumn(n));
            col++;
        }
        LocatedRow lr = new LocatedRow(locatedRow.getRowLocation(), row);
        return lr;
    }


    public KeyEncoder getKeyEncoder() throws StandardException {
        HashPrefix prefix;
        DataHash dataHash;
        KeyPostfix postfix = NoOpPostfix.INSTANCE;
        if(pkCols==null){
            prefix = new SaltedPrefix(EngineDriver.driver().newUUIDGenerator(100));
            dataHash = NoOpDataHash.INSTANCE;
        }else{
            int[] keyColumns = new int[pkCols.length];
            for(int i=0;i<keyColumns.length;i++){
                keyColumns[i] = pkCols[i] -1;
            }
            prefix = NoOpPrefix.INSTANCE;
            DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(execRowDefinition);
            dataHash = BareKeyHash.encoder(keyColumns,null, SpliceKryoRegistry.getInstance(),serializers);
        }
        return new KeyEncoder(prefix,dataHash,postfix);
    }

    public DataHash getRowHash() throws StandardException {
        //get all columns that are being set
        int[] columns = getEncodingColumns(execRowDefinition.nColumns(),pkCols);
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(execRowDefinition);
        return new EntryDataHash(columns,null,serializers);
    }

    public static int[] getEncodingColumns(int n, int[] pkCols) {
        int[] columns = IntArrays.count(n);
        // Skip primary key columns to save space
        if (pkCols != null) {
            for(int pkCol:pkCols) {
                columns[pkCol-1] = -1;
            }
        }
        return columns;
    }

}
