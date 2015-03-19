package com.splicemachine.pipeline.writecontextfactory;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeAddColumnDesc;
import com.splicemachine.derby.impl.sql.execute.altertable.AddColumnRowTransformer;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.pipeline.writehandler.altertable.AddColumnInterceptWriteHandler;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.Pair;

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
        AddColumnRowTransformer rowTransformer = new AddColumnRowTransformer(generateTemplateRows(descriptor),
                                                                             createKeyDecoder(descriptor),
                                                                             createRowDecoder(descriptor),
                                                                             createRowEncoder(descriptor));
        AddColumnInterceptWriteHandler handler = new AddColumnInterceptWriteHandler(rowTransformer, newTableName);
        if (ddlChange == null) {
            ctx.addLast(handler);
        } else {
            DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(ddlChange.getTxn());
            ctx.addLast(new SnapshotIsolatedWriteHandler(handler, ddlFilter));
        }
    }

    @Override
    public long getConglomerateId() {
        return descriptor.getConglomerateNumber();
    }

    private static Pair<ExecRow,ExecRow> generateTemplateRows(TentativeAddColumnDesc descriptor) {
        ColumnInfo[] columnInfos = descriptor.getColumnInfos();
        ExecRow oldRow = new ValueRow(columnInfos.length);
        ExecRow newRow = new ValueRow(columnInfos.length+1);

        try {
            for (int i=0; i<columnInfos.length-1; i++) {
                DataValueDescriptor dvd = columnInfos[i].dataType.getNull();
                oldRow.setColumn(i,dvd);
                newRow.setColumn(i,dvd);
            }
            DataValueDescriptor newColDVD = columnInfos[columnInfos.length].defaultValue;
            newRow.setColumn(columnInfos.length, (newColDVD != null ? newColDVD : columnInfos[columnInfos.length].dataType.getNull()));
        } catch (StandardException e) {
            // FIXME: JC
        }
        return Pair.newPair(oldRow,newRow);
    }

    private static KeyHashDecoder createKeyDecoder(TentativeAddColumnDesc descriptor) {
        // TODO: JC
        return null;
    }

    private static EntryDataDecoder createRowDecoder(TentativeAddColumnDesc descriptor) {
        // TODO: JC
        return null;
    }

    private static PairEncoder createRowEncoder(TentativeAddColumnDesc descriptor) {
        // TODO: JC
        return null;
    }
}
