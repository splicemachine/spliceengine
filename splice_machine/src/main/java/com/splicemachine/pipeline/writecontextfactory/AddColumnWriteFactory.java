package com.splicemachine.pipeline.writecontextfactory;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeAddColumnDesc;
import com.splicemachine.derby.impl.sql.execute.altertable.AddColumnRowTransformer;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.pipeline.writehandler.altertable.AddColumnInterceptWriteHandler;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;

public class AddColumnWriteFactory implements LocalWriteFactory {
    private final DDLChange ddlChange;
    private final TentativeAddColumnDesc descriptor;
    private final byte[] newTableName;

    private AddColumnWriteFactory(DDLChange ddlChange, TentativeAddColumnDesc descriptor) {
        this.ddlChange = ddlChange;
        this.descriptor = descriptor;
        this.newTableName = Bytes.toBytes(String.valueOf(descriptor.getConglomerateNumber()));
    }

    public static AddColumnWriteFactory create(DDLChange ddlChange) {
        if (ddlChange.getChangeType() != DDLChangeType.ADD_COLUMN) {
            return null;
        }

        return new AddColumnWriteFactory(ddlChange, (TentativeAddColumnDesc) ddlChange.getTentativeDDLDesc());
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        String tableVersion;
        try {
            tableVersion = DataDictionaryUtils.getTableVersion(ddlChange.getTxn(), descriptor.getTableId());
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        int[] columnOrdering = DataDictionaryUtils.getColumnOrdering(ddlChange.getTxn(), descriptor.getTableId());

        AddColumnRowTransformer rowTransformer =
            AddColumnRowTransformer.create(tableVersion, columnOrdering, descriptor.getColumnInfos());

        AddColumnInterceptWriteHandler handler = new AddColumnInterceptWriteHandler(rowTransformer, newTableName);

        DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(ddlChange.getTxn());
        ctx.addLast(new SnapshotIsolatedWriteHandler(handler, ddlFilter));
    }

    @Override
    public long getConglomerateId() {
        return descriptor.getConglomerateNumber();
    }
}
