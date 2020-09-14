package com.splicemachine.derby.impl.sql.compile.calcite;

import com.splicemachine.db.iapi.sql.compile.ConvertSelectContext;
import com.splicemachine.db.impl.sql.compile.SelectNode;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

public class ConvertSelectContextImpl implements ConvertSelectContext {
    private SelectNode root;
    private RelNode relRoot;
    Map<Integer, Integer> startColPosMap = null;
    private RelNode leftJoinSource;
    private RelNode rightJoinSource;

    public ConvertSelectContextImpl(SelectNode selectNode) {
        root = selectNode;
    }

    public void setStartColPosMap(Map<Integer, Integer> map) {
        startColPosMap = map;
    }

    public Map<Integer, Integer> getStartColPosMap() {
        return startColPosMap;
    }
    public RelNode getRelRoot() {
        return relRoot;
    }
    public SelectNode getSelectRoot() {
        return root;
    }
    public void setRelRoot(RelNode relRoot) {
        this.relRoot = relRoot;
    }
    public RelNode getLeftJoinSource() {
        return leftJoinSource;
    }
    public RelNode getRightJoinSource() {
        return rightJoinSource;
    }
    public void setLeftJoinSource(RelNode source) {
        leftJoinSource = source;
    }
    public void setRightJoinSource(RelNode source) {
        rightJoinSource = source;
    }
}
