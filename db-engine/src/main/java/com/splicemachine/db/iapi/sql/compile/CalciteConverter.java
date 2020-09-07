package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;

public interface CalciteConverter {
   RelBuilder getRelBuilder();
   SqlOperator mapRelationalOperatorToCalciteSqlOperator(int operator) throws StandardException;
   RelDataType mapToRelDataType(DataTypeDescriptor dtd);
   RelOptCluster getCluster();

}
