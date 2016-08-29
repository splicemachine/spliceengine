/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.ast.ISpliceVisitor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.sql.compile.ASTVisitor;
import com.splicemachine.db.iapi.sql.compile.CompilationPhase;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import org.spark_project.guava.collect.ImmutableMap;

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
    public final Map<CompilationPhase, List<Class<? extends ISpliceVisitor>>> visitorClasses;

    public SpliceASTWalker(List<Class<? extends ISpliceVisitor>> afterParseClasses,
                           List<Class<? extends ISpliceVisitor>> afterBindClasses,
                           List<Class<? extends ISpliceVisitor>> afterOptimizeClasses) {
        visitorClasses = ImmutableMap.of(
                CompilationPhase.AFTER_PARSE, afterParseClasses,
                CompilationPhase.AFTER_BIND, afterBindClasses,
                CompilationPhase.AFTER_OPTIMIZE, afterOptimizeClasses);
    }


    @Override
    public void begin(String statementText, CompilationPhase phase) throws StandardException {
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
    public void end(CompilationPhase phase) throws StandardException {
        for(ASTVisitor v: visitors){
            v.end(phase);
        }
        visitors.clear();
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
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
