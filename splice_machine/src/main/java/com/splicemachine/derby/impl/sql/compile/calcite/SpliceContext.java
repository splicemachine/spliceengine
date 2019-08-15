package com.splicemachine.derby.impl.sql.compile.calcite;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.sql.compile.ResultColumn;
import com.splicemachine.utils.Pair;
import org.apache.calcite.plan.Context;

import java.util.HashMap;

/**
 * Created by yxia on 8/20/19.
 */
public class SpliceContext implements Context {
    LanguageConnectionContext lcc;
    CompilerContext cc;
    HashMap<Pair<Integer, Integer>, ResultColumn> baseColumnMap = new HashMap<>();


    public SpliceContext(LanguageConnectionContext lcc, CompilerContext cc) {
        this.lcc = lcc;
        this.cc = cc;
    }

    public LanguageConnectionContext getLcc() {
        return lcc;
    }

    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        return null;
    }

    public NodeFactory getNodeFactory(){
        return lcc.getLanguageConnectionFactory().getNodeFactory();
    }

    public CompilerContext getCompilerContext() {
        return cc;
    }

    public ContextManager getContextManager() {
        return cc.getContextManager();
    }

    public CostEstimate newCostEstimate() throws StandardException {
        return lcc.getOptimizerFactory().getCostEstimate();
    }

    /** save the result column corresponding to a base table column, they will be used when
     *  converting RelOptTree back to Derby tree for code generation
     * @param columnId
     * @param rc
     */
    public void addBaseColumn(Pair<Integer, Integer> columnId, ResultColumn rc) {
        baseColumnMap.put(columnId, rc);

    }

    public ResultColumn getBaseColumn(Pair<Integer, Integer> columnId) {
        return baseColumnMap.get(columnId);
    }
}
