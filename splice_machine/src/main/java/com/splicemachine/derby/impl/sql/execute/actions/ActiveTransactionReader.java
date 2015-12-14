package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.si.api.SIFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.Streams;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class ActiveTransactionReader {
    private final long minTxnId;
    private final long maxTxnId;
    private final byte[] writeTable;
    private static final SIFactory siFactory = SIDriver.getSIFactory();

    public ActiveTransactionReader(long minTxnId, long maxTxnId, byte[] writeTable){
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.writeTable = writeTable;
    }

    public Stream<TxnView> getAllTransactions() throws IOException{
        return Streams.empty(); // TODO JL
    }

    public Stream<TxnView> getActiveTransactions()
            throws IOException{
        return Streams.empty(); // TODO JL
    }

}
