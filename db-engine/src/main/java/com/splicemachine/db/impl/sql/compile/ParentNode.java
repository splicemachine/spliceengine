package com.splicemachine.db.impl.sql.compile;

import java.util.List;

public interface ParentNode {
    List<? extends QueryTreeNode> getChildren();

    QueryTreeNode getChild(int index);

    void setChild(int index, QueryTreeNode newValue);
}
