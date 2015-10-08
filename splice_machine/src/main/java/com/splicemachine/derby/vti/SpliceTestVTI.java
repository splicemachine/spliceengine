package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
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
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
       HBaseRowLocation rl = new HBaseRowLocation();
       ValueRow valueRow = new ValueRow(4);
        valueRow.setColumn(1,new SQLInteger(2));
        valueRow.setColumn(2,new SQLVarchar("foo"));
        valueRow.setColumn(3,new SQLVarchar("foo2"));
        valueRow.setColumn(4,new SQLVarchar("foo3"));
       return StreamUtils.controlDataSetProcessor.singleRowDataSet(new LocatedRow(rl,valueRow));
    }

}
