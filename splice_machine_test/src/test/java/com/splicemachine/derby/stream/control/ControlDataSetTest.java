package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.BooleanDataValue;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.*;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.primitives.Bytes;
import org.junit.*;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by jleach on 4/15/15.
 */
public class ControlDataSetTest extends AbstractDataSetTest {

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new ControlDataSet<>(tenRowsTwoDuplicateRecords);
    }

}