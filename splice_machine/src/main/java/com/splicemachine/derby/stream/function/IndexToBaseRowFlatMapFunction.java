package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowReader;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowReaderBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowToBaseRowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 5/28/15.
 */
public class IndexToBaseRowFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,Iterable<LocatedRow>,LocatedRow> {
    boolean initialized;
    protected IndexRowToBaseRowOperation indexRowToBaseRowOperation;
    protected IndexRowReaderBuilder indexRowReaderBuilder;
    protected IndexRowReader reader;


    public IndexToBaseRowFlatMapFunction() {
        super();
    }

    public IndexToBaseRowFlatMapFunction(OperationContext<Op> operationContext,
                                         IndexRowReaderBuilder indexRowReaderBuilder) {
        super(operationContext);
        this.indexRowReaderBuilder = indexRowReaderBuilder;
    }


    @Override
    public Iterable<LocatedRow> call(Iterable<LocatedRow> locatedRows) throws Exception {
        if (!initialized) {
            indexRowToBaseRowOperation = (IndexRowToBaseRowOperation) getOperation();
            reader = indexRowReaderBuilder.source(locatedRows).build();
            initialized = true;
        }
        indexRowToBaseRowOperation.registerCloseable(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                reader.close();
            }
        });
        return reader;
    }
}
