package com.splicemachine.derby.vti.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by jleach on 10/7/15.
 */
public interface DatasetProvider {
    /**
     *
     * Processing pipeline supporting in memory and spark datasets.
     *
     * @param dsp
     * @param execRow
     * @param <Op>
     * @return
     * @throws StandardException
     */
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException;

    /**
     *
     * Dynamic MetaData used to dynamically bind a function.
     *
     * @return
     */
    public ResultSetMetaData getMetaData() throws SQLException;
}
