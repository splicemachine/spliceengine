package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.util.Arrays;


public class HBaseController  extends SpliceController {
    protected static Logger LOG = Logger.getLogger(HBaseController.class);

    public HBaseController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
        super(openSpliceConglomerate, trans);
    }

    @Override
    public int insert(DataValueDescriptor[] row) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("insert into conglom %d row %s with txnId %s",
                    openSpliceConglomerate.getConglomerate().getContainerid(),(row==null ? null : Arrays.toString(row)),trans.getTxnInformation()));
        Table htable = getTable();
        try {
            Put put = SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());

            encodeRow(row,put,null,null);
            htable.put(put);
            return 0;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void insertAndFetchLocation(DataValueDescriptor[] row,
                                       RowLocation destRowLocation) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("insertAndFetchLocation into conglom %d row %s",
                    openSpliceConglomerate.getConglomerate().getContainerid(),row==null? null :Arrays.toString(row)));

        Table htable = getTable();
        try {
            Put put = SpliceUtils.createPut(SpliceUtils.getUniqueKey(), ((SpliceTransaction)trans).getTxn());
            encodeRow(row, put, null, null);
            destRowLocation.setValue(put.getRow());
            htable.put(put);
        } catch (Exception e) {
            throw StandardException.newException("insert and fetch location error",e);
        }
    }

    @Override
    public boolean replace(RowLocation loc, DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("replace rowLocation %s, destRow %s, validColumns %s",loc,(row==null ? null : Arrays.toString(row)),validColumns));
        Table htable = getTable();
        try {

            Put put = SpliceUtils.createPut(loc.getBytes(),((SpliceTransaction)trans).getTxn());

            encodeRow(row, put, SpliceUtils.bitSetToMap(validColumns), validColumns);
            htable.put(put);
            return true;
        } catch (Exception e) {
            throw StandardException.newException("Error during replace ",e);
        }
    }
}
