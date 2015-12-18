package com.splicemachine.pipeline.writehandler.foreignkey;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.storage.DataScan;

import java.io.IOException;
import java.util.List;

/**
 * Perform the FK existence check on a referencing child table's backing index.  Fails write if a referencing
 * row DOES exist--preventing an UPDATE or DELETE in the parent table.
 */
public class ForeignKeyChildCheckWriteHandler implements WriteHandler {

    private final TransactionalRegion transactionalRegion;
    private final TxnOperationFactory txnOperationFactory;
    private final ServerControl env;
    private final FKConstraintInfo fkConstraintInfo;
    private final SDataLib dataLib;

    public ForeignKeyChildCheckWriteHandler(TransactionalRegion transactionalRegion,
                                            ServerControl env,
                                            FKConstraintInfo fkConstraintInfo,
                                            SDataLib dataLib) {
        this.transactionalRegion = transactionalRegion;
        this.env = env;
        this.fkConstraintInfo = fkConstraintInfo;
//        this.txnOperationFactory = transactionalRegion.getOperationFactory();
        this.txnOperationFactory = null;
        this.dataLib = dataLib;
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

//        TxnFilter txnFilter = transactionalRegion.unpackedFilter(ctx.getTxn());
//        SIFilter siFilter = new SIFilter(txnFilter);
//        PrefixFilter prefixFilter = new PrefixFilter(startKey);

        DataScan scan = txnOperationFactory.newDataScan(ctx.getTxn());
//        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
        scan.startKey(startKey);
        throw new UnsupportedOperationException("IMPLEMENT");
//        scan.setFilter(new FilterList(prefixFilter, siFilter));
//
//        List result = Lists.newArrayList();
//        RegionScanner regionScanner = env.getRegion().getScanner(scan);
//        try {
//            dataLib.regionScannerNext(regionScanner, result);
//        } finally {
//            regionScanner.close();
//        }
//        return result;
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