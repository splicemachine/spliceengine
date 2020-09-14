package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.impl.sql.compile.SelectNode;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

public interface ConvertSelectContext {
    Map<Integer, Integer> getStartColPosMap();
    void setStartColPosMap(Map<Integer, Integer> map);
    RelNode getRelRoot();
    SelectNode getSelectRoot();
    void setRelRoot(RelNode root);
    RelNode getLeftJoinSource();
    RelNode getRightJoinSource();
    void setLeftJoinSource(RelNode source);
    void setRightJoinSource(RelNode source);
}
