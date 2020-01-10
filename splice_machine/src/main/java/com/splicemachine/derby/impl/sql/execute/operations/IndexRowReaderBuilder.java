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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpKeyHashDecoder;
import com.splicemachine.derby.utils.marshall.SkippingKeyDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.EntryPredicateFilter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 4/11/14
 */
public class IndexRowReaderBuilder implements Externalizable{
    private Iterator<ExecRow> source;
    private int lookupBatchSize;
    private int numConcurrentLookups=-1;
    private ExecRow outputTemplate;
    private long mainTableConglomId=-1;
    private int[] mainTableRowDecodingMap;
    private FormatableBitSet mainTableAccessedRowColumns;
    private int[] mainTableKeyColumnEncodingOrder;
    private boolean[] mainTableKeyColumnSortOrder;
    private int[] mainTableKeyDecodingMap;
    private FormatableBitSet mainTableAccessedKeyColumns;
    private int[] execRowTypeFormatIds;
    /*
     * A Map from the physical location of the Index columns in the INDEX scanned row
     * and the decoded output row.
     */
    private int[] indexCols;
    private String tableVersion;
    private int[] mainTableKeyColumnTypes;
    private TxnView txn;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder indexColumns(int[] indexCols){
        this.indexCols=indexCols;
        return this;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds){
        this.execRowTypeFormatIds=execRowTypeFormatIds;
        return this;
    }


    public IndexRowReaderBuilder mainTableVersion(String mainTableVersion){
        this.tableVersion=mainTableVersion;
        return this;
    }

    public IndexRowReaderBuilder source(Iterator<ExecRow> source){
        this.source=source;
        return this;
    }

    public IndexRowReaderBuilder lookupBatchSize(int lookupBatchSize){
        this.lookupBatchSize=lookupBatchSize;
        return this;
    }

    public IndexRowReaderBuilder numConcurrentLookups(int numConcurrentLookups){
        this.numConcurrentLookups=numConcurrentLookups;
        return this;
    }

    public IndexRowReaderBuilder outputTemplate(ExecRow outputTemplate){
        this.outputTemplate=outputTemplate;
        return this;
    }

    public IndexRowReaderBuilder transaction(TxnView transaction){
        this.txn=transaction;
        return this;
    }

    public IndexRowReaderBuilder mainTableConglomId(long mainTableConglomId){
        this.mainTableConglomId=mainTableConglomId;
        return this;
    }

    public IndexRowReaderBuilder mainTableAccessedRowColumns(FormatableBitSet mainTableAccessedRowColumns){
        this.mainTableAccessedRowColumns=mainTableAccessedRowColumns;
        return this;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder mainTableRowDecodingMap(int[] mainTableRowDecodingMap){
        this.mainTableRowDecodingMap=mainTableRowDecodingMap;
        return this;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder mainTableKeyColumnEncodingOrder(int[] mainTableKeyColumnEncodingOrder){
        this.mainTableKeyColumnEncodingOrder=mainTableKeyColumnEncodingOrder;
        return this;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder mainTableKeyColumnTypes(int[] mainTableKeyColumnTypes){
        this.mainTableKeyColumnTypes=mainTableKeyColumnTypes;
        return this;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder mainTableKeyColumnSortOrder(boolean[] mainTableKeyColumnSortOrder){
        this.mainTableKeyColumnSortOrder=mainTableKeyColumnSortOrder;
        return this;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public IndexRowReaderBuilder mainTableKeyDecodingMap(int[] mainTableKeyDecodingMap){
        this.mainTableKeyDecodingMap=mainTableKeyDecodingMap;
        return this;
    }

    public IndexRowReaderBuilder mainTableAccessedKeyColumns(FormatableBitSet mainTableAccessedKeyColumns){
        this.mainTableAccessedKeyColumns=mainTableAccessedKeyColumns;
        return this;
    }

    public IndexRowReader build() throws StandardException{
        assert txn!=null:"No Transaction specified!";
        assert mainTableRowDecodingMap!=null:"No Row decoding map specified!";
        assert mainTableConglomId>=0:"No Main table conglomerate id specified!";
        assert outputTemplate!=null:"No output template specified!";
        assert source!=null:"No source specified";
        assert indexCols!=null:"No index columns specified!";

        BitSet rowFieldsToReturn=new BitSet(mainTableAccessedRowColumns.getNumBitsSet());
        for(int i=mainTableAccessedRowColumns.anySetBit();i>=0;i=mainTableAccessedRowColumns.anySetBit(i)){
            rowFieldsToReturn.set(i);
        }
        EntryPredicateFilter epf=new EntryPredicateFilter(rowFieldsToReturn);
        byte[] epfBytes=epf.toBytes();

        DescriptorSerializer[] templateSerializers=VersionedSerializers.forVersion(tableVersion,false).getSerializers(outputTemplate);
        KeyHashDecoder keyDecoder;
        if(mainTableKeyColumnEncodingOrder==null || mainTableAccessedKeyColumns==null || mainTableAccessedKeyColumns.getNumBitsSet()<=0)
            keyDecoder=NoOpKeyHashDecoder.INSTANCE;
        else{
            keyDecoder=SkippingKeyDecoder.decoder(VersionedSerializers.typesForVersion(tableVersion),
                    templateSerializers,
                    mainTableKeyColumnEncodingOrder,
                    mainTableKeyColumnTypes,
                    mainTableKeyColumnSortOrder,
                    mainTableKeyDecodingMap,
                    mainTableAccessedKeyColumns);
        }

        KeyHashDecoder rowDecoder=new EntryDataDecoder(mainTableRowDecodingMap,null,templateSerializers);

        SIDriver driver=SIDriver.driver();
        TxnOperationFactory txnOperationFactory =driver.getOperationFactory();
        PartitionFactory tableFactory = driver.getTableFactory();
        return new IndexRowReader(
                source,
                outputTemplate,
                txn,
                lookupBatchSize,
                Math.max(numConcurrentLookups,2),
                mainTableConglomId,
                epfBytes,
                keyDecoder,
                rowDecoder,
                indexCols,
                txnOperationFactory,
                tableFactory);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        SIDriver driver=SIDriver.driver();

        driver.getOperationFactory().writeTxn(txn,out);
        out.writeInt(lookupBatchSize);
        out.writeInt(numConcurrentLookups);
        out.writeObject(outputTemplate);
        out.writeLong(mainTableConglomId);
        ArrayUtil.writeIntArray(out,mainTableRowDecodingMap);
        out.writeObject(mainTableAccessedRowColumns);
        ArrayUtil.writeIntArray(out,mainTableKeyColumnEncodingOrder);
        if(mainTableKeyColumnSortOrder!=null){
            out.writeBoolean(true);
            ArrayUtil.writeBooleanArray(out,mainTableKeyColumnSortOrder);
        }else{
            out.writeBoolean(false);
        }
        ArrayUtil.writeIntArray(out,mainTableKeyDecodingMap);
        out.writeObject(mainTableAccessedKeyColumns);
        ArrayUtil.writeIntArray(out,indexCols);
        out.writeUTF(tableVersion);
        ArrayUtil.writeIntArray(out,mainTableKeyColumnTypes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        txn=SIDriver.driver().getOperationFactory().readTxn(in);
        lookupBatchSize=in.readInt();
        numConcurrentLookups=in.readInt();
        outputTemplate = (ExecRow) in.readObject();
        mainTableConglomId=in.readLong();
        mainTableRowDecodingMap=ArrayUtil.readIntArray(in);
        mainTableAccessedRowColumns=(FormatableBitSet)in.readObject();
        mainTableKeyColumnEncodingOrder=ArrayUtil.readIntArray(in);
        if(in.readBoolean()){
            mainTableKeyColumnSortOrder=ArrayUtil.readBooleanArray(in);
        }
        mainTableKeyDecodingMap=ArrayUtil.readIntArray(in);
        mainTableAccessedKeyColumns=(FormatableBitSet)in.readObject();
        indexCols=ArrayUtil.readIntArray(in);
        tableVersion=in.readUTF();
        mainTableKeyColumnTypes=ArrayUtil.readIntArray(in);
    }
}
