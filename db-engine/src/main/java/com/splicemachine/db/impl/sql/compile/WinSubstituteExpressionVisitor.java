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

package com.splicemachine.db.impl.sql.compile;

import org.apache.log4j.Logger;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;

/**
 * Replaces an equivalent <em>source</em> expression with a <em>target</em>
 * expression.
 */
class WinSubstituteExpressionVisitor extends SubstituteExpressionVisitor {
    private ResultColumn srcRC;
    private Logger LOG;

    WinSubstituteExpressionVisitor(ValueNode s, ValueNode t, Class skipThisClass, Logger LOG) {
        super(s, t, skipThisClass);
        srcRC = source.getSourceResultColumn();
        if (LOG.isDebugEnabled()) {
            LOG.debug("SubstituteVisitor: Source RC is "+srcRC);
        }
        this.LOG = LOG;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof ValueNode)) {
            return node;
        }

        ValueNode nd = (ValueNode) node;
        if (srcRC != null && nd instanceof ColumnReference && source instanceof ColumnReference) {
            // ColumnReference#isEquvalent() compares node "tableNumber"s. When pulling up column
            // references in WindowResultSetNode (pre-optimise), we don't have a tableNumber yet.
            // This behavior compares ColumnReference column descriptors.
            ResultColumn ndRC = nd.getSourceResultColumn();
            if (ndRC != null && ndRC.getTableColumnDescriptor().isEquivalent(srcRC.getTableColumnDescriptor())) {
                ++numSubstitutions;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SubstituteVisitor: Replacing (non-null source RC) "+nd);
                }
                return target;
            }
        } else if (nd instanceof ColumnReference && source instanceof ColumnReference) {
            if (source.getColumnName() != null && source.getColumnName().endsWith("Result")) {
                ResultColumn ndRC = ((ColumnReference)nd).getSource();
                if (ndRC != null && ndRC.getExpression().isEquivalent(source)) {
                    ++numSubstitutions;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SubstituteVisitor: Replacing (null source RC) "+nd);
                    }
                    return target;
                }
            }
        } else if (nd.isEquivalent(source)) {
            ++numSubstitutions;
            if (LOG.isDebugEnabled()) {
                LOG.debug("SubstituteVisitor: Replacing (generic) "+nd);
            }
            return target;
        }
        return node;
    }
}
