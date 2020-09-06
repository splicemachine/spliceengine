package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;

public interface CalciteConverter {
   RelBuilder getRelBuilder();
   SqlOperator mapRelationalOperatorToCalciteSqlOperator(int operator) throws StandardException;
}
