package com.splicemachine.derby.management;

/**
 * Created by jyuan on 5/12/14.
 */

import com.splicemachine.db.iapi.error.StandardException;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface XPlainTracePrinter {
    ResultSet print() throws SQLException, StandardException, IllegalAccessException;
}
