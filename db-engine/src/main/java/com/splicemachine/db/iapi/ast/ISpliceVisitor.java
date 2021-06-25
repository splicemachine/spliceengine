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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilationPhase;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.*;

/**
 * Visitor interface with methods for each of Derby's Visitable nodes (which make up the QueryTreeNode hierarchy).
 *
 * User: pjt
 * Date: 7/9/13
 */
public interface ISpliceVisitor {
    void setContext(String query, CompilationPhase phase);
    boolean isPostOrder();
    boolean stopTraversal();
    boolean skipChildren(Visitable node);

    /**
     * Return the low-level Visitor instead of the wrapper class
     * in case this is an ASTVisitor.
     */
    default ISpliceVisitor getVisitor() { return this; }

    Visitable defaultVisit(Visitable node) throws StandardException;

    // ResultSet nodes
    Visitable visit(DistinctNode node) throws StandardException;
    Visitable visit(FromBaseTable node) throws StandardException;
    Visitable visit(FromSubquery node) throws StandardException;
    Visitable visit(GroupByNode node) throws StandardException;
    Visitable visit(HalfOuterJoinNode node) throws StandardException;
    Visitable visit(IndexToBaseRowNode node) throws StandardException;
    Visitable visit(IntersectOrExceptNode node) throws StandardException;
    Visitable visit(JoinNode node) throws StandardException;
    Visitable visit(MaterializeResultSetNode node) throws StandardException;
    Visitable visit(OrderByNode node) throws StandardException;
    Visitable visit(ProjectRestrictNode node) throws StandardException;
    Visitable visit(RowCountNode node) throws StandardException;
    Visitable visit(RowResultSetNode node) throws StandardException;
    Visitable visit(UnionNode node) throws StandardException;
    Visitable visit(WindowResultSetNode node) throws StandardException;

    // Rest
    Visitable visit(AndNode node) throws StandardException;
    Visitable visit(DeleteNode node) throws StandardException;
    Visitable visit(OrNode node) throws StandardException;
    Visitable visit(StatementNode node) throws StandardException;
    Visitable visit(SubqueryNode node) throws StandardException;
    Visitable visit(UpdateNode node) throws StandardException;
    Visitable visit(ExplainNode node) throws StandardException;
    Visitable visit(ExportNode node) throws StandardException;
    Visitable visit(KafkaExportNode node) throws StandardException;
    Visitable visit(FullOuterJoinNode node) throws StandardException;
}
