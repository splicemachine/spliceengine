package com.splicemachine.derby.impl.sql.execute.AlterTable;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/12/14
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */

import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.hbase.writer.WriteResult;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.log4j.Logger;
import org.apache.derby.catalog.UUID;
import org.apache.hadoop.hbase.KeyValue;
import java.io.IOException;
import java.util.List;

public class DropColumnHandler implements WriteHandler {

    private static Logger LOG = Logger.getLogger(DropColumnHandler.class);

    private RecordingCallBuffer<KVPair> writeBuffer;
    private UUID tableId;
    private long toConglomId;
    private String txnId;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;
    private RowTransformer rowTransformer;
    private ConglomerateLoader loader;
    private boolean failed;

    public DropColumnHandler(UUID tableId,
                             long toConglomId,
                             String txnId,
                             ColumnInfo[] columnInfos,
                             int droppedColumnPosition) {
        this.tableId = tableId;
        this.toConglomId = toConglomId;
        this.txnId = txnId;
        this.columnInfos = columnInfos;
        this.droppedColumnPosition = droppedColumnPosition;
        rowTransformer = new RowTransformer(tableId, txnId, columnInfos, droppedColumnPosition);
        loader = new ConglomerateLoader(toConglomId, txnId);
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {

        try {
            KVPair newPair = null;
            if (mutation.getType() == KVPair.Type.DELETE){
                newPair = mutation;
            }
            else {
                // create a KeyValue for the mutation
                KeyValue kv = mutation.toKeyValue();
                newPair = rowTransformer.transform(kv);
            }
            loader.add(newPair);
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }

        if (!failed) {
            ctx.sendUpstream(mutation);
        }

    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new RuntimeException("Not Supported");
    }

    @Override
    public void finishWrites(WriteContext ctx) throws IOException {
        try {
            loader.flush();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
