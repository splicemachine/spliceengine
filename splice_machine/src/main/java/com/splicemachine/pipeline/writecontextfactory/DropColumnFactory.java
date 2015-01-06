package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeDropColumnDesc;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.DropColumnHandler;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.impl.sql.execute.ColumnInfo;

import java.io.IOException;

class DropColumnFactory implements LocalWriteFactory {
    private UUID tableId;
    private TxnView txn;
    private long newConglomId;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;
    private DDLChange ddlChange;

    public DropColumnFactory(UUID tableId,
                             TxnView txn,
                             long newConglomId,
                             ColumnInfo[] columnInfos,
                             int droppedColumnPosition,
                             DDLChange ddlChange) {
        this.tableId = tableId;
        this.txn = txn;
        this.newConglomId = newConglomId;
        this.columnInfos = columnInfos;
        this.droppedColumnPosition = droppedColumnPosition;
        this.ddlChange = ddlChange;
    }

    public static DropColumnFactory create(DDLChange ddlChange) {
        if (ddlChange.getChangeType() != DDLChangeType.DROP_COLUMN)
            return null;

        TentativeDropColumnDesc desc = (TentativeDropColumnDesc) ddlChange.getTentativeDDLDesc();

        UUID tableId = desc.getTableId();
        TxnView txn = ddlChange.getTxn();
        long newConglomId = desc.getConglomerateNumber();
        ColumnInfo[] columnInfos = desc.getColumnInfos();
        int droppedColumnPosition = desc.getDroppedColumnPosition();
        return new DropColumnFactory(tableId, txn, newConglomId, columnInfos, droppedColumnPosition, ddlChange);
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        DropColumnHandler handler = new DropColumnHandler(tableId, newConglomId, txn, columnInfos, droppedColumnPosition);
        if (ddlChange == null) {
            ctx.addLast(handler);
        } else {
            DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(ddlChange.getTxn());
            ctx.addLast(new SnapshotIsolatedWriteHandler(handler, ddlFilter));
        }
    }

    @Override
    public long getConglomerateId() {
        return newConglomId;
    }
}
