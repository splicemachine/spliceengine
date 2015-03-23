package com.splicemachine.derby.impl.ast;

import com.google.common.collect.ImmutableMap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.sql.compile.ASTVisitor;
import com.splicemachine.db.iapi.sql.compile.Visitable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of Derby's ASTVisitor interface which runs multiple Splice visitors
 * for each of Derby's "ASTWalker" phases:
 * <p/>
 * 1 after parsing
 * 2 after binding &
 * 3 after optimization
 * <p/>
 * User: pjt
 * Date: 7/5/13
 */
public class SpliceASTWalker implements ASTVisitor {
    private List<ASTVisitor> visitors = new ArrayList<ASTVisitor>();
    public final Map<Integer, List<Class<? extends ISpliceVisitor>>> visitorClasses;

    public SpliceASTWalker(List<Class<? extends ISpliceVisitor>> afterParseClasses,
                           List<Class<? extends ISpliceVisitor>> afterBindClasses,
                           List<Class<? extends ISpliceVisitor>> afterOptimizeClasses) {
        visitorClasses = ImmutableMap.of(
                ASTVisitor.AFTER_PARSE, afterParseClasses,
                ASTVisitor.AFTER_BIND, afterBindClasses,
                ASTVisitor.AFTER_OPTIMIZE, afterOptimizeClasses);
    }


    @Override
    public void begin(String statementText, int phase) throws StandardException {
        for (Class c : visitorClasses.get(phase)) {
            try {
                ASTVisitor v = new SpliceDerbyVisitorAdapter((ISpliceVisitor) c.newInstance());
                v.begin(statementText, phase);
                visitors.add(v);
            } catch (InstantiationException | IllegalAccessException e) {
                throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                        String.format("Problem instantiating SpliceVisitor %s", c.getSimpleName()));
            }
        }
    }

    @Override
    public void end(int phase) throws StandardException {
        for(ASTVisitor v: visitors){
            v.end(phase);
        }
        visitors.clear();
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        for (ASTVisitor v : visitors) {
            node = node.accept(v);
        }
        return node;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        // Always return true, i.e. visit only the root node
        return true;
    }

    @Override
    public void initializeVisitor() throws StandardException {
    }

    @Override
    public void teardownVisitor() throws StandardException {
    }
}
