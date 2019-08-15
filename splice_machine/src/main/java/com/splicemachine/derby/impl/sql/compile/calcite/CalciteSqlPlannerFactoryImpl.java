package com.splicemachine.derby.impl.sql.compile.calcite;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.SqlPlanner;
import com.splicemachine.db.iapi.sql.compile.SqlPlannerFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import org.apache.calcite.plan.Context;

/**
 * Created by yxia on 9/17/19.
 */
public class CalciteSqlPlannerFactoryImpl implements SqlPlannerFactory {
    public SqlPlanner getSqlPlanner(Context context) throws StandardException {
        SpliceContext sc = context.unwrap(SpliceContext.class);
        return new CalciteSqlPlanner(sc);
    }

    public Context newContext(LanguageConnectionContext lcc, CompilerContext cc) {
        return new SpliceContext(lcc, cc);
    }
}
