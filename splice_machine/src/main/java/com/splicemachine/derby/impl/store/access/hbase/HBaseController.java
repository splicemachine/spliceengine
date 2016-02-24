package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.Partition;
import org.apache.log4j.Logger;

import java.util.Arrays;


public class HBaseController extends SpliceController{
    protected static final Logger LOG=Logger.getLogger(HBaseController.class);

    public HBaseController(OpenSpliceConglomerate openSpliceConglomerate,
                           Transaction trans,
                           PartitionFactory partitionFactory,
                           TxnOperationFactory txnOperationFactory){
        super(openSpliceConglomerate,trans,partitionFactory,txnOperationFactory);
    }

    @Override
    public int insert(DataValueDescriptor[] row) throws StandardException{
        assert row!=null: "Cannot insert a null row!";
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("insert into conglom %d row %s with txnId %s",
                    openSpliceConglomerate.getConglomerate().getContainerid(),(Arrays.toString(row)),trans.getTxnInformation()));
        Partition htable=getTable();
        try{
            DataPut put=opFactory.newDataPut(trans.getTxnInformation(),SpliceUtils.getUniqueKey());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());

            encodeRow(row,put,null,null);
            htable.put(put);
            return 0;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void insertAndFetchLocation(DataValueDescriptor[] row,
                                       RowLocation destRowLocation) throws StandardException{
        assert row!=null: "Cannot insert into a null row!";
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("insertAndFetchLocation into conglom %d row %s",
                    openSpliceConglomerate.getConglomerate().getContainerid(),Arrays.toString(row)));

        Partition htable=getTable(); //-sf- don't want to close the htable here, it might break stuff
        try{
            DataPut put=opFactory.newDataPut(trans.getTxnInformation(),SpliceUtils.getUniqueKey());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());
            encodeRow(row,put,null,null);
            destRowLocation.setValue(put.key());
            htable.put(put);
        }catch(Exception e){
            throw StandardException.newException("insert and fetch location error",e);
        }
    }

    @Override
    public boolean replace(RowLocation loc,DataValueDescriptor[] row,FormatableBitSet validColumns) throws StandardException{
        assert row!=null: "Cannot replace with a null row";
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("replace rowLocation %s, destRow %s, validColumns %s",loc,Arrays.toString(row),validColumns));
        Partition htable=getTable();
        try{

            DataPut put=opFactory.newDataPut(trans.getTxnInformation(),loc.getBytes());//SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());

            encodeRow(row,put,EngineUtils.bitSetToMap(validColumns),validColumns);
            htable.put(put);
            return true;
        }catch(Exception e){
            throw StandardException.newException("Error during replace ",e);
        }
    }
}
