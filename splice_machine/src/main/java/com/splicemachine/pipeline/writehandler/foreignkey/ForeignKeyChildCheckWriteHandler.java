package com.splicemachine.pipeline.writehandler.foreignkey;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.SIFilter;
import com.splicemachine.si.api.filter.TxnFilter;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import java.io.IOException;
import java.util.List;

/**
 * Perform the FK existence check on a referencing child table's backing index.  Fails write if a referencing
 * row DOES exist--preventing an UPDATE or DELETE in the parent table.
 */
public class ForeignKeyChildCheckWriteHandler implements WriteHandler {

    private final TransactionalRegion transactionalRegion;
    private final TxnOperationFactory txnOperationFactory;
    private final RegionCoprocessorEnvironment env;
    private final FKConstraintInfo fkConstraintInfo;
    private final SDataLib dataLib;

    public ForeignKeyChildCheckWriteHandler(TransactionalRegion transactionalRegion, RegionCoprocessorEnvironment env, FKConstraintInfo fkConstraintInfo) {
        this.transactionalRegion = transactionalRegion;
        this.env = env;
        this.fkConstraintInfo = fkConstraintInfo;
        this.txnOperationFactory = TransactionOperations.getOperationFactory();
        this.dataLib = HTransactorFactory.getTransactor().getDataLib();
    }

    @Override
    public void next(KVPair kvPair, WriteContext ctx) {
        // I only do foreign key checks.
        if (kvPair.getType() == KVPair.Type.FOREIGN_KEY_CHILDREN_EXISTENCE_CHECK) {
            if (!transactionalRegion.rowInRange(kvPair.getRowKey())) {
                // The row would not longer be in this region, if it did/does exist.
                ctx.failed(kvPair, WriteResult.wrongRegion());
            } else {
                try {
                    List rowsReferencingParent = scanForReferences(kvPair, ctx);
                    if (!rowsReferencingParent.isEmpty()) {
                        String failedKvAsHex = Bytes.toHex(kvPair.getRowKey());
                        ConstraintContext context = ConstraintContext.foreignKey(fkConstraintInfo).withInsertedMessage(0, failedKvAsHex);
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

    private List scanForReferences(KVPair kvPair, WriteContext ctx) throws IOException {
        byte[] startKey = kvPair.getRowKey();

        TxnFilter txnFilter = transactionalRegion.unpackedFilter(ctx.getTxn());
        SIFilter siFilter = new SIFilter(txnFilter);
        PrefixFilter prefixFilter = new PrefixFilter(startKey);

        Scan scan = txnOperationFactory.newScan(ctx.getTxn());
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
        scan.setStartRow(startKey);
        scan.setFilter(new FilterList(prefixFilter, siFilter));

        List result = Lists.newArrayList();
        RegionScanner regionScanner = env.getRegion().getScanner(scan);
        try {
            dataLib.regionScannerNext(regionScanner, result);
        } finally {
            regionScanner.close();
        }
        return result;
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