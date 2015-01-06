package com.splicemachine.pipeline.writehandler;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnOperationFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.List;

/**
 * Perform the actual FK existence check on a referenced primary key column or unique index.
 */
public class ForeignKeyCheckWriteHandler implements WriteHandler {

    private TransactionalRegion region;
    private TxnOperationFactory txnOperationFactory;
    private RegionCoprocessorEnvironment env;
    private int constraintColumnCount;

    public ForeignKeyCheckWriteHandler(TransactionalRegion region, RegionCoprocessorEnvironment env, int constraintColumnCount) {
        this.region = region;
        this.env = env;
        this.constraintColumnCount = constraintColumnCount;
        this.txnOperationFactory = TransactionOperations.getOperationFactory();
    }

    /**
     * TODO: Need to use batch gets here, DB-2582 is added to the FK epic to address this.
     */
    @Override
    public void next(KVPair kvPair, WriteContext ctx) {
        // I only do foreign key checks.
        if (kvPair.getType() == KVPair.Type.FOREIGN_KEY_CHECK) {
            if (!region.rowInRange(kvPair.getRow())) {
                // The row would not longer be in this region, if it did/does exist.
                ctx.failed(kvPair, WriteResult.wrongRegion());
            } else {
                try {
                    byte[] targetRowKey = getCheckRowKey(kvPair.getRow(), constraintColumnCount);

                    Get get = txnOperationFactory.newGet(ctx.getTxn(), targetRowKey);
                    Result result = env.getRegion().get(get);
                    if (result.isEmpty()) {
                        // ConstraintContext will be replaced later where we have child table name, etc.
                        ConstraintContext context = ConstraintContext.empty();
                        WriteResult foreignKeyConstraint = new WriteResult(Code.FOREIGN_KEY_VIOLATION, context);
                        ctx.failed(kvPair, foreignKeyConstraint);
                    } else {
                        ctx.success(kvPair);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        ctx.sendUpstream(kvPair);
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new UnsupportedOperationException("never called");
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
    }

    /**
     * If the backing indexes is non-unique then there will be more columns in the KVPair rowKey than exist
     * in the referenced column.
     *
     * Unfortunate because in that case we create a new byte[] for each KV. DB-2582 is crated to address this.
     * Finally, we may actually need to use an decoder here to handle types that contain zeros.
     *
     * rowKeyIn           = [65, 67, 0 54, 45, 0, 0, 0]
     * constraintColCount = 2
     */
    private byte[] getCheckRowKey(byte[] rowKeyIn, int constraintColCount) {
        int colCount = 0;
        for (int i = 0; i < rowKeyIn.length; i++) {
            if (rowKeyIn[i] == 0) {
                colCount++;
            }
            if (i == rowKeyIn.length - 1) {
                return rowKeyIn;
            }
            if (colCount == constraintColCount) {
                byte[] checkRowKey = new byte[i];
                System.arraycopy(rowKeyIn, 0, checkRowKey, 0, i);
                return checkRowKey;
            }
        }
        throw new IllegalStateException();
    }


}
