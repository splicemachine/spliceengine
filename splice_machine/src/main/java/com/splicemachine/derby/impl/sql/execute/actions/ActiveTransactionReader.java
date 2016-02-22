package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.Streams;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class ActiveTransactionReader {
    private final long minTxnId;
    private final long maxTxnId;
    private final byte[] writeTable;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ActiveTransactionReader(long minTxnId, long maxTxnId, byte[] writeTable){
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.writeTable = writeTable;
    }

    public Stream<TxnView> getAllTransactions() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public Stream<TxnView> getActiveTransactions()
            throws IOException{
        //TODO -sf- there may be millions here, so we need to be careful
        return Streams.wrap(SIDriver.driver().getTxnStore().getActiveTransactions(minTxnId,maxTxnId,writeTable));
    }

}
