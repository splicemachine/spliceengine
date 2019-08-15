package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import org.apache.calcite.plan.Context;


/**
 * Created by yxia on 9/17/19.
 */
public interface SqlPlannerFactory {
    /**
     Module name for the monitor's module locating system.
     */
    String MODULE = "com.splicemachine.db.iapi.sql.compile.SqlPlannerFactory";

    SqlPlanner getSqlPlanner(Context context) throws StandardException;

    public Context newContext(LanguageConnectionContext lcc, CompilerContext cc);
}
