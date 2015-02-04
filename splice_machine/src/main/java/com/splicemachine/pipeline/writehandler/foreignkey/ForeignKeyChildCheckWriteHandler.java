package com.splicemachine.pipeline.writehandler.foreignkey;

import com.google.common.collect.Lists;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * Perform the FK existence check on a referencing child table's backing index.  Fails write if a referencing
 * row DOES exist--preventing an UPDATE or DELETE in the parent table.
 */
public class ForeignKeyChildCheckWriteHandler implements WriteHandler {

    private final TransactionalRegion region;
    private final TxnOperationFactory txnOperationFactory;
    private final RegionCoprocessorEnvironment env;
    private final ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor;
    private final SDataLib dataLib;

    public ForeignKeyChildCheckWriteHandler(TransactionalRegion region, RegionCoprocessorEnvironment env, ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor) {
        this.region = region;
        this.env = env;
        this.foreignKeyConstraintDescriptor = foreignKeyConstraintDescriptor;
        this.txnOperationFactory = TransactionOperations.getOperationFactory();
        this.dataLib = HTransactorFactory.getTransactor().getDataLib();
    }

    @Override
    public void next(KVPair kvPair, WriteContext ctx) {
        // I only do foreign key checks.
        if (kvPair.getType() == KVPair.Type.FOREIGN_KEY_CHILDREN_EXISTENCE_CHECK) {
            if (!region.rowInRange(kvPair.getRowKey())) {
                // The row would not longer be in this region, if it did/does exist.
                ctx.failed(kvPair, WriteResult.wrongRegion());
            } else {
                try {
                    Scan scan = txnOperationFactory.newScan(ctx.getTxn());
                    byte[] startKey = kvPair.getRowKey();
                    scan.setStartRow(startKey);
                    scan.setFilter(new PrefixFilter(startKey));

                    List result = Lists.newArrayList();
                    RegionScanner regionScanner = env.getRegion().getScanner(scan);
                    dataLib.regionScannerNext(regionScanner, result);

                    if (!result.isEmpty()) {
                        String failedKvAsHex = BytesUtil.toHex(kvPair.getRowKey());
                        ConstraintContext context = ConstraintContext.foreignKey(foreignKeyConstraintDescriptor).withInsertedMessage(0, failedKvAsHex);
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


}