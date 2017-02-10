/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.ast.ISpliceVisitor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.sql.compile.ASTVisitor;
import com.splicemachine.db.iapi.sql.compile.CompilationPhase;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import org.apache.log4j.Logger;
import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
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

    private static Visitable invokeVisit(ISpliceVisitor visitor, Visitable node) throws StandardException {
        final Class<? extends Visitable> nClass = node.getClass();
        try {
            Method m = methods.get(nClass, new Callable<Method>() {
                @Override
                public Method call() throws Exception {
                    Method m = ISpliceVisitor.class.getMethod("visit", nClass);
                    m.setAccessible(true);
                    return m;
                }
            });
            return (Visitable) m.invoke(visitor, node);

        } catch (ExecutionException e) {
            throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                                                    String.format("Problem finding ISpliceVisitor visit method for %s",
                                                                     nClass));
        } catch (IllegalAccessException | InvocationTargetException e) {
             throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                                                    String.format("Problem invoking ISpliceVisitor visit method for %s",
                                                                     nClass));
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
    public void begin(String statementText, CompilationPhase phase) throws StandardException {
        v.setContext(statementText, phase);
        start = System.nanoTime();
    }

    @Override
    public void end(CompilationPhase phase) throws StandardException {
        if (LOG.isDebugEnabled()) {
            float duration = (System.nanoTime() - start) / 1000000f;
            LOG.debug(String.format("%s visited %d nodes in %.2f ms", v.getClass().getSimpleName(), visited, duration));
        }
        visited = 0;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
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
