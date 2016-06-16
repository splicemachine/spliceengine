package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;

import java.io.IOException;
import java.util.List;

/**
 * Perform the FK existence check on a referencing child table's backing index.  Fails write if a referencing
 * row DOES exist--preventing an UPDATE or DELETE in the parent table.
 */
public class ForeignKeyChildCheckWriteHandler implements WriteHandler{

    private final TransactionalRegion transactionalRegion;
    private final TxnOperationFactory txnOperationFactory;
    private final FKConstraintInfo fkConstraintInfo;

    public ForeignKeyChildCheckWriteHandler(TransactionalRegion transactionalRegion,
                                            FKConstraintInfo fkConstraintInfo,
                                            TxnOperationFactory txnOperationFactory) {
        this.transactionalRegion = transactionalRegion;
        this.fkConstraintInfo = fkConstraintInfo;
        this.txnOperationFactory = txnOperationFactory;
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
                    if(hasReferences(kvPair,ctx)){
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

    private boolean hasReferences(KVPair kvPair,WriteContext ctx) throws IOException {
        byte[] startKey = kvPair.getRowKey();

        //make sure this is a transactional scan
        DataScan scan = txnOperationFactory.newDataScan(ctx.getTxn());
        scan =scan.startKey(startKey);
        /*
         * The way prefix keys work is that longer keys sort after shorter keys. We
         * are already starting exactly where we want to be, and we want to end as soon
         * as we hit a record which is not this key.
         *
         * Historically, we did this by using an HBase PrefixFilter. We can do that again,
         * but it's a bit of a pain to make that work in an architecture-independent
         * way (we would need to implement a version of that for other architectures,
         * for example. It's much easier for us to just make use of row key sorting
         * to do the job for us.
         *
         * We start where we want, and we need to end as soon as we run off that. The
         * first key which is higher than the start key is the start key as a prefix followed
         * by 0x00 (in unsigned sort order). Therefore, we make the end key
         * [startKey | 0x00].
         */
        byte[] endKey = Bytes.unsignedCopyAndIncrement(startKey);//new byte[startKey.length+1];
//        System.arraycopy(startKey,0,endKey,0,startKey.length);
        scan = scan.stopKey(endKey);

        try(DataScanner scanner = ctx.getRegion().openScanner(scan)){
            List<DataCell> next=scanner.next(1); //all we need is one row to be good
            if(next.size()<=0) return false;
            TxnFilter txnFilter=ctx.txnRegion().unpackedFilter(ctx.getTxn());
            int cellCount = next.size();
            for(DataCell dc:next){
                DataFilter.ReturnCode rC = txnFilter.filterCell(dc);
                switch(rC){
                    case NEXT_ROW:
                        return false; //the entire row is filtered
                    case SKIP:
                    case NEXT_COL:
                    case SEEK:
                        cellCount--; //the cell is filtered
                        break;
                    case INCLUDE:
                    case INCLUDE_AND_NEXT_COL: //the cell is included
                    default:
                        break;
                }
            }
            return cellCount>0;
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
    }


}