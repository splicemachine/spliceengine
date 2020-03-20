package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StatementListNode extends StatementNode implements Iterable<StatementNode> {
    private ArrayList<StatementNode> list = new ArrayList<>();
    @Override
    public String statementToString() {
        StringBuilder sb = new StringBuilder();
        for (StatementNode s: list) {
            sb.append(s.statementToString());
            sb.append(", ");
        }
        return sb.toString();
    }

    @Override
    int activationKind() {
        assert false: "activationKind should not be called for StatementList";
        return 0;
    }

    void addStatement(StatementNode statementNode) {
        list.add(statementNode);
    }

    int size() {
        return list.size();
    }

    @Override
    public Iterator<StatementNode> iterator() {
        return list.iterator();
    }

    @Override
    public void printSubNodes(int depth){
        if(SanityManager.DEBUG){
            for (int index = 0; index < size(); index++) {
                debugPrint(formatNodeString("[" + index + "]:", depth));
                list.get(index).treePrint(depth);
            }
        }
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        for (StatementNode s: list) {
            s.accept(v, this);
        }
    }

    public StatementNode get(int index) {
        return list.get(index);
    }

    public void set(int index, StatementNode statementNode) {
        list.set(index, statementNode);
    }
}
