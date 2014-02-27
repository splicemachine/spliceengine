package com.splicemachine.derby.impl.ast;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
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
    private static final Logger LOG = Logger.getLogger(SpliceDerbyVisitorAdapter.class);

    ISpliceVisitor v;

    long start;
    int visited;

    // Method lookup
    private static Cache<Class, Method> methods = CacheBuilder.newBuilder().build();

    private static Visitable invokeVisit(ISpliceVisitor visitor, Visitable node)
            throws StandardException {
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
            throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                                                    String.format("Problem finding ISpliceVisitor visit method for %s",
                                                                     nClass));
        } catch (IllegalAccessException e) {
             throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                                                    String.format("Problem invoking ISpliceVisitor visit method for %s",
                                                                     nClass));
        } catch (InvocationTargetException e) {
            throw Exceptions.parseException(e);
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
        if (LOG.isDebugEnabled()) {
            float duration = (System.nanoTime() - start) / 1000000f;
            LOG.debug(String.format("%s visited %d nodes in %.2f ms", v.getClass().getSimpleName(), visited, duration));
        }
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
