package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.derby.vti.iapi.DatasetProvider;

/**
 * Created by jleach on 10/7/15.
 */
public class SpliceTestVTI implements DatasetProvider {

    public static DatasetProvider getSpliceTestVTI() {
        return new SpliceTestVTI();
    }

    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp,ExecRow execRow) throws StandardException {
       return StreamUtils.controlDataSetProcessor.singleRowDataSet(new LocatedRow(execRow));
    }

}
