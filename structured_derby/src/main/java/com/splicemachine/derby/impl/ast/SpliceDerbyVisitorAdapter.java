package com.splicemachine.derby.impl.ast;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * This class is a bridge between Derby's Visitor interface (which has a single visit method)
 * and the Splice Visitor interface (which has a visit method for each of Derby's Visitable classes).
 *
 * User: pjt
 * Date: 7/9/13
 */
public class SpliceDerbyVisitorAdapter implements ASTVisitor {
    private Logger LOG = Logger.getLogger(SpliceDerbyVisitorAdapter.class);

    ISpliceVisitor v;

    long start;
    int visited;

    // Method lookup
    private static Cache<Class, Method> methods = CacheBuilder.newBuilder().build();

    private static Visitable invokeVisit(ISpliceVisitor visitor, Visitable node) {
        final Class<? extends Visitable> nClass = node.getClass();
        try {
            Method m = methods.get(nClass, new Callable<Method>() {
                @Override
                public Method call() throws Exception {
                    Method m = ISpliceVisitor.class.getMethod("visit", new Class[]{nClass});
                    m.setAccessible(true);
                    return m;
                }
            });
            return (Visitable) m.invoke(visitor, node);

        } catch (ExecutionException e) {
            throw new RuntimeException(String.format("Problem finding ISpliceVisitor visit method for %s", nClass), e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(String.format("Problem invoking ISpliceVisitor visit method for %s", nClass), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("Problem invoking ISpliceVisitor visit method for %s", nClass), e);
        }
    }


    public SpliceDerbyVisitorAdapter(ISpliceVisitor v) {
        this.v = v;
    }

    @Override
    public void initializeVisitor() throws StandardException {
    }

    @Override
    public void teardownVisitor() throws StandardException {
    }

    @Override
    public void begin(String statementText, int phase) throws StandardException {
        v.setContext(statementText, phase);
        start = System.nanoTime();
    }

    @Override
    public void end(int phase) throws StandardException {
        long duration = start - System.nanoTime();
        LOG.info(String.format("%s visited %d nodes in %d ms", v.getClass().getSimpleName(), visited, (duration / 1000000)));
        visited = 0;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        visited++;
        return invokeVisit(v, node);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return v.isPostOrder();
    }

    @Override
    public boolean stopTraversal() {
        return v.stopTraversal();
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return v.skipChildren(node);
    }
}
