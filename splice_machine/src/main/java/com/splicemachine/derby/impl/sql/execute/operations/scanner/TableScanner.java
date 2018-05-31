package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.utils.StandardIterator;

/**
 * Created by jleach on 10/11/17.
 */
public interface TableScanner extends StandardIterator<ExecRow>,AutoCloseable {


    public RowLocation getCurrentRowLocation();

}
