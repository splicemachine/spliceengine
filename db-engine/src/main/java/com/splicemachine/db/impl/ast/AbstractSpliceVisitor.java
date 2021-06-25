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

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilationPhase;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.iapi.ast.ISpliceVisitor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Default ISpliceVisitor implementation, which provides identity behavior for each of Derby's Visitable nodes.
 * Default behavior is encoded in the defaultVisit, which subclasses can override in addition to individual
 * visit() methods.
 *
 * User: pjt
 * Date: 7/9/13
 */
public abstract class AbstractSpliceVisitor implements ISpliceVisitor {
    String query;

    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "query field is used in PlanPrinter subclass")
    @Override
    public void setContext(String query, CompilationPhase ignored) {
        this.query = query;
    }

    @Override
    public boolean isPostOrder() {
        // Default to PostOrder traversal, i.e. bottom-up
        return true;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {
        return node;
    }

    @Override
    public Visitable visit(DistinctNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromBaseTable node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromSubquery node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(HalfOuterJoinNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IndexToBaseRowNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IntersectOrExceptNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(JoinNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(MaterializeResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowCountNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AndNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DeleteNode node) throws StandardException {
        return defaultVisit(node);
    }

     @Override
    public Visitable visit(OrNode node) throws StandardException {
        return defaultVisit(node);
    }

    // don't delete
    @Override
    public Visitable visit(StatementNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SubqueryNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UpdateNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExplainNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExportNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(KafkaExportNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
	public Visitable visit(OrderByNode node) throws StandardException {
        return defaultVisit(node);
	}

    @Override
    public Visitable visit(FullOuterJoinNode node) throws StandardException {
        return defaultVisit(node);
    }
}
