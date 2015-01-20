package com.splicemachine.pipeline.writehandler;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnOperationFactory;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.List;

/**
 * Perform the actual FK existence check on a referenced primary key column or unique index.
 */
public class ForeignKeyCheckWriteHandler implements WriteHandler {

    private final TransactionalRegion region;
    private final TxnOperationFactory txnOperationFactory;
    private final RegionCoprocessorEnvironment env;

    /* FormatIds of just the FK columns. */
    private final int formatIds[];
    private final MultiFieldDecoder multiFieldDecoder;

    public ForeignKeyCheckWriteHandler(TransactionalRegion region, RegionCoprocessorEnvironment env, int[] formatIds) {
        this.region = region;
        this.env = env;
        this.formatIds = formatIds;
        this.txnOperationFactory = TransactionOperations.getOperationFactory();
        this.multiFieldDecoder = MultiFieldDecoder.create();
    }

    /**
     * TODO: Need to use batch gets here, DB-2582 is added to the FK epic to address this.
     */
    @Override
    public void next(KVPair kvPair, WriteContext ctx) {
        // I only do foreign key checks.
        if (kvPair.getType() == KVPair.Type.FOREIGN_KEY_CHECK) {
            if (!region.rowInRange(kvPair.getRowKey())) {
                // The row would not longer be in this region, if it did/does exist.
                ctx.failed(kvPair, WriteResult.wrongRegion());
            } else {
                try {
                    byte[] targetRowKey = getCheckRowKey(kvPair.getRowKey());
                    // targetRowKey == null means that the referencing row contained at least one null, in which
                    // case FK rules say it can never be a violation, the insert/update is allowed.
                    if (targetRowKey != null) {

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
     * The rowKey we get in this class, via the write pipeline, is the row key we are attempting to write to
     * the FK backing index.  We have to account for two major things before checking for its existence in the
     * referenced primary key or unique index:
     *
     * (1)
     * If the FK backing indexes is non-unique (the default, always the case if there is not also a unique constraint
     * on the FK column) then there will be more columns (appended) in the KVPair rowKey than exist in the referenced
     * primary-key/index because of the way we encode rowKeys in non-unique indexes. Unfortunate because in that case we
     * create a new byte array for each KV. DB-2582 exists to see if we can avoid this (possible performance optimization).
     *
     * (2)
     * We have to use a MultiFieldDecoder here to determine if any of the columns in the index are null.  Per the spec
     * we do not check FK constraints on child rows if any col in the FK is null. We have to use MultiFieldDecoder
     * even if we know there are no nulls (because of a not-null constraint for example) in order to correctly
     * count the columns and apply the logic described in #1.
     *
     * Example (two col FK where the primary-key or unique-index to check ends in '45'):
     *
     * rowKeyIn          = [65, 67, 0 54, 45, 0, bytes, to, make, index-entry, unique]
     * formatIds.length  = 2
     */
    private byte[] getCheckRowKey(byte[] rowKeyIn) {

        int position = 0;
        multiFieldDecoder.set(rowKeyIn);
        for (int i = 0; i < formatIds.length; i++) {
            if (multiFieldDecoder.nextIsNull()) {
                return null;
            }
            if (formatIds[i] == StoredFormatIds.SQL_DOUBLE_ID) {
                position += multiFieldDecoder.skipDouble();
            } else if (formatIds[i] == StoredFormatIds.SQL_REAL_ID) {
                position += multiFieldDecoder.skipFloat();
            } else if (formatIds[i] == StoredFormatIds.SQL_LONGINT_ID) {
                position += multiFieldDecoder.skipLong();
            } else {
                position += multiFieldDecoder.skip();
            }
        }
        int lastKeyIndex = position - 2;

        if (lastKeyIndex == rowKeyIn.length - 1) {
            return rowKeyIn;
        }
        byte[] checkRowKey = new byte[lastKeyIndex + 1];
        System.arraycopy(rowKeyIn, 0, checkRowKey, 0, lastKeyIndex + 1);
        return checkRowKey;
    }

}
