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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;

/**
 *
 * This visitor will push down limits and offsets down to determine costs
 * at the appropriate level.
 *
 */
public class LimitOffsetVisitor extends AbstractSpliceVisitor {
    private static Logger LOG=Logger.getLogger(LimitOffsetVisitor.class);
    public long offset = -1;
    public long fetchFirst = -1;
    public double scaleFactor;
    /**
     *
     * Visiting the RowCountNode
     *
     * @param node
     * @return
     * @throws StandardException
     */
    @Override
    public Visitable visit(RowCountNode node) throws StandardException {
        offset = fetchNumericValue(node.offset);
        fetchFirst = fetchNumericValue(node.fetchFirst);
        scaleFactor = 1.0d;
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("rowCountNode ={%s}, offset=%s, fetchFirst=%s", node, offset, fetchFirst));
        return super.visit(node);
    }

    public static long fetchNumericValue(ValueNode valueNode) throws StandardException {
        if (valueNode != null && valueNode instanceof NumericConstantNode)
            return ((NumericConstantNode)valueNode).getValue().getLong();
        return -1L;
    }

    /**
     * Adjusts the base table cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */
    @Override
    public Visitable visit(IndexToBaseRowNode node) throws StandardException {
        adjustBaseTableCost(node);
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the base table cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(FromBaseTable node) throws StandardException {
        if (!node.isDistinctScan())
            adjustBaseTableCost(node);
        else
            adjustCost(node);
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(DistinctNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }


    @Override
    public Visitable visit(FromSubquery node) throws StandardException {
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(GroupByNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(JoinNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(HalfOuterJoinNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }

    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */
    @Override
    public Visitable visit(FullOuterJoinNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(RowResultSetNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(IntersectOrExceptNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }

    @Override
    public Visitable visit(MaterializeResultSetNode node) throws StandardException {
        nullify();
        return super.visit(node);
    }

    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(UnionNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }

    @Override
    public Visitable visit(WindowResultSetNode node) throws StandardException {
        nullify();
        return super.visit(node);
    }
    /**
     * Adjusts the remote rows and cost based on the limit, removes limit elements
     *
     * @param node
     * @return
     * @throws StandardException
     */

    @Override
    public Visitable visit(OrderByNode node) throws StandardException {
        adjustCost(node);
        nullify();
        return super.visit(node);
    }

    @Override
    public Visitable visit(ExportNode node) throws StandardException {
        nullify();
        return super.visit(node);
    }

    @Override
    public Visitable visit(BinaryExportNode node) throws StandardException {
        nullify();
        return super.visit(node);
    }

    @Override
    public Visitable visit(SubqueryNode node) throws StandardException {
        nullify();
        return super.visit(node);
    }

    /**
     * {@inheritDoc}
     * @return {@code false}, since the tree should be walked top down
     */
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    /**
     * Top Down ordering to see how far to push limits
     */
    public boolean isPostOrder() {
        return false;
    }

    /**
     * Remove the limit items
     *
     */
    private void nullify() {
        offset = -1;
        fetchFirst = -1;
    }

    /**
     *
     * Adjust normal node costing, row count and remote cost reduction
     *
     * Need to account for offset
     *
     * @param rsn
     * @throws StandardException
     */

    public void adjustCost(ResultSetNode rsn) throws StandardException {
        if (fetchFirst==-1 && offset ==-1) // No Limit Adjustment
            return;
        CostEstimate costEstimate = rsn.getFinalCostEstimate(false);
        long totalRowCount = costEstimate.getEstimatedRowCount();
        long currentOffset = offset==-1?0:offset;
        long currentFetchFirst = fetchFirst==-1?totalRowCount:fetchFirst;
        scaleFactor = (double) currentFetchFirst/(double) totalRowCount;
        if (scaleFactor >= 1.0d) {
            // do nothing, will not effect cost
        } else {
            costEstimate.setEstimatedRowCount(currentOffset+currentFetchFirst);
            costEstimate.setRemoteCost(scaleFactor*costEstimate.getRemoteCost());
            costEstimate.setRemoteCostPerPartition(costEstimate.remoteCost(), costEstimate.partitionCount());
        }

    }

    /**
     *
     * Adjust base table costing...  Attacks BaseTable, Projections, and Index Lookups, etc.
     *
     * Need to account for offset better.
     *
     * @param rsn
     * @throws StandardException
     */
    public void adjustBaseTableCost(ResultSetNode rsn) throws StandardException {
        if (fetchFirst==-1 && offset ==-1) // No Limit Adjustment
            return;
        CostEstimate costEstimate = rsn.getFinalCostEstimate(false);
        long totalRowCount = costEstimate.getEstimatedRowCount();
        long currentOffset = offset==-1?0:offset;
        long currentFetchFirst = fetchFirst==-1?totalRowCount:fetchFirst;
        scaleFactor = (double) currentFetchFirst/(double) totalRowCount;
        if (scaleFactor >= 1.0d) {
                // do nothing, will not effect cost
        } else {
                costEstimate.setEstimatedRowCount(currentFetchFirst);
                costEstimate.setSingleScanRowCount(currentFetchFirst);
                costEstimate.setRowCount(currentFetchFirst);
                costEstimate.setProjectionRows(currentFetchFirst);
                costEstimate.setIndexLookupRows(costEstimate.getIndexLookupRows()*scaleFactor);
                costEstimate.setEstimatedHeapSize((long)(costEstimate.getEstimatedHeapSize()*scaleFactor));
                costEstimate.setRemoteCost((long)(costEstimate.getRemoteCost()*scaleFactor));
                costEstimate.setFromBaseTableCost(costEstimate.getFromBaseTableCost()*scaleFactor);
                costEstimate.setFromBaseTableRows(costEstimate.getFromBaseTableRows()*scaleFactor);
                costEstimate.setScannedBaseTableRows(costEstimate.getScannedBaseTableRows()*scaleFactor);
                costEstimate.setEstimatedCost(costEstimate.getEstimatedCost()*scaleFactor);
                costEstimate.setRemoteCostPerPartition(costEstimate.remoteCost(), costEstimate.partitionCount());
        }
    }


/*

<!-- This was the original code in the RowCountNode -->

    public void fixCost() throws StandardException {
        if (fetchFirst != null && fetchFirst instanceof NumericConstantNode) {
            long totalRowCount = costEstimate.getEstimatedRowCount();
            long fetchCount = ((NumericConstantNode)fetchFirst).getValue().getInt();
            double factor = (double)fetchCount/(double)totalRowCount;
            costEstimate.setEstimatedRowCount(fetchCount);
            costEstimate.setSingleScanRowCount(fetchCount);
            costEstimate.setEstimatedHeapSize((long)(costEstimate.getEstimatedHeapSize()*factor));
            costEstimate.setRemoteCost((long)(costEstimate.getRemoteCost()*factor));
        }
        else
        if (offset != null && offset instanceof NumericConstantNode) {
            long totalRowCount = costEstimate.getEstimatedRowCount();
            long offsetCount = ((NumericConstantNode)offset).getValue().getInt();
            costEstimate.setEstimatedRowCount(totalRowCount-offsetCount >=1? totalRowCount-offsetCount:1); // Snap to 1
        } else {
            // Nothing
        }
    }

*/
}
