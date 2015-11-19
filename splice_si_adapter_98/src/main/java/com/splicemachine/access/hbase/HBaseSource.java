package com.splicemachine.access.hbase;

import com.splicemachine.access.iapi.SpliceConnectionFactory;
import com.splicemachine.access.iapi.SpliceSource;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.impl.txnclient.CoprocessorTxnStore;
import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseSource implements SpliceSource {
    private HBaseTableFactory hBaseTableFactory;
    private HBaseTableInfoFactory hBaseTableInfoFactory;
    private HBaseConnectionFactory hBaseConnectionFactory;

    private static HBaseSource INSTANCE = new HBaseSource();

    private HBaseSource() {
        hBaseTableInfoFactory = HBaseTableInfoFactory.getInstance();
        hBaseConnectionFactory = HBaseConnectionFactory.getInstance();
        hBaseTableFactory = HBaseTableFactory.getInstance();

    }

    public static HBaseSource getInstance() {
        return INSTANCE;
    }

    @Override
    public SpliceConnectionFactory getSpliceConnectionFactory() throws IOException {
        return hBaseConnectionFactory;
    }

    @Override
    public HBaseTableFactory getSpliceTableFactory() throws IOException {
        return hBaseTableFactory;
    }

    @Override
    public HBaseTableInfoFactory getSpliceTableInfoFactory() throws IOException {
        return hBaseTableInfoFactory;
    }


    @Override
    public CoprocessorTxnStore getTxnStore(TimestampSource ts) throws IOException {
        return new CoprocessorTxnStore(hBaseTableFactory,ts,null);
    }
}