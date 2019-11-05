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

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.store.access.StoreCostResult;

/**
 * A CostEstimate represents the cost of getting a ResultSet, along with the
 * ordering of rows in the ResultSet, and the estimated number of rows in
 * this ResultSet.
 *
 */

public interface CostEstimate extends StoreCostResult {
    /**
     * Set the cost for this cost estimate.
     */
    void setCost(double cost, double rowCount, double singleScanRowCount);

    void setCost(double cost, double rowCount, double singleScanRowCount,int numPartitions);

    void setRemoteCost(double remoteCost);

    void setLocalCost(double remoteCost);

    /**
     *
     *  Key flag to identify join type for computing join selectivity.
     *
     * @return
     */
    int getJoinType();

    boolean isOuterJoin();
    /**
     *
     * Set the flag on the cost so the join selectivity algorithm can understand if you are an outer or innner join.
     * Anti-join is handled via another mechanism.
     *
     * @param joinType
     */
    void setJoinType(int joinType);

    /**
     *
     *  Key flag to identify join type for computing join selectivity.
     *
     * @return
     */
    boolean isAntiJoin();

    /**
     *
     * Set the flag on the cost so the join selectivity algorithm can understand if you are an outer or innner join.
     *
     *
     * @param isAntiJoin
     */
    void setAntiJoin(boolean isAntiJoin);

    /**
     * Copy the values from the given cost estimate into this one.
     */
    void setCost(CostEstimate other);

    /**
     * Set the single scan row count.
     */
    void setSingleScanRowCount(double singleRowScanCount);

    void setNumPartitions(int numPartitions);

    /**
     * Compare this cost estimate with the given cost estimate.
     *
     * @param other		The cost estimate to compare this one with
     *
     * @return	< 0 if this < other, 0 if this == other, > 0 if this > other
     */
    double compare(CostEstimate other);

    double compareLocal(CostEstimate other);

    /**
     * Add this cost estimate to another one.  This presumes that any row
     * ordering is destroyed.
     *
     * @param addend	This cost estimate to add this one to.
     * @param retval	If non-null, put the result here.
     *
     * @return  this + other.
     */
    CostEstimate add(CostEstimate addend, CostEstimate retval);

    /**
     * Multiply this cost estimate by a scalar, non-dimensional number.  This
     * presumes that any row ordering is destroyed.
     *
     * @param multiplicand	The value to multiply this CostEstimate by.
     * @param retval	If non-null, put the result here.
     *
     * @return	this * multiplicand
     */
    CostEstimate multiply(double multiplicand, CostEstimate retval);

    /**
     * Divide this cost estimate by a scalar, non-dimensional number.
     *
     * @param divisor	The value to divide this CostEstimate by.
     * @param retval	If non-null, put the result here.
     *
     * @return	this / divisor
     */
    CostEstimate divide(double divisor, CostEstimate retval);

    /**
     * Get the estimated number of rows returned by the ResultSet that this
     * CostEstimate models.
     */
    double rowCount();

    /**
     * Get the estimated number of rows returned by a single scan of
     * the ResultSet that this CostEstimate models.
     */
    double singleScanRowCount();

    /**
     * @return the number of partitions which must be visited.
     */
    int partitionCount();

    double remoteCost();

    double localCost();

    void setEstimatedHeapSize(long estHeapBytes);

    long getEstimatedHeapSize();

    /** Get a copy of this CostEstimate */
    CostEstimate cloneMe();

    /**
     * Return whether or not this CostEstimate is uninitialized.
     *
     * @return Whether or not this CostEstimate is uninitialized.
     */
    boolean isUninitialized();

    RowOrdering getRowOrdering();

    void setRowOrdering(RowOrdering rowOrdering);

    OptimizablePredicateList getPredicateList();

    void setPredicateList(OptimizablePredicateList predList);

    CostEstimate getBase();

    void setBase(CostEstimate baseCost);

    /**
     * @return true if this is a "real" cost--that is, a cost which was generated
     * using real statistics, rather than from arbitrary scaling factors
     */
    boolean isRealCost();

    void setIsRealCost(boolean isRealCost);

    /**
     * @return the cost to open a scan and begin reading data
     */
    double getOpenCost();

    void setOpenCost(double openCost);

    /**
     * @return the cost to close a scan after completely reading data
     */
    double getCloseCost();

    void setCloseCost(double closeCost);

    void setRowCount(double outerRows);

    /**
     * @return a well-formatted display string
     */
    String prettyProcessingString();

    String prettyProcessingString(String attrDelim);

    /**
     * @return a well-formatted display string
     */
    String prettyScrollInsensitiveString();

    String prettyScrollInsensitiveString(String attrDelim);

    String prettyFromBaseTableString();

    String prettyFromBaseTableString(String attrDelim);

    String prettyIndexLookupString();

    String prettyIndexLookupString(String attrDelim);

    String prettyProjectionString();

    String prettyProjectionString(String attrDelim);

    String prettyDmlStmtString(String rowsLabel);

    String prettyDmlStmtString(double cost, long rows, String attrDelim, String rowsLabel);

    double getProjectionRows();

    void setProjectionRows(double projectionRows);

    double getProjectionCost();

    void setProjectionCost(double projectionCost);

    double getIndexLookupRows() ;

    void setIndexLookupRows(double indexLookupRows) ;

    double getIndexLookupCost() ;

    void setIndexLookupCost(double indexLookupCost) ;

    double getFromBaseTableRows() ;

    void setFromBaseTableRows(double fromBaseTableRows);

    double getScannedBaseTableRows() ;

    void setScannedBaseTableRows(double scannedBaseTableRows);

    double getFromBaseTableCost();

    void setFromBaseTableCost(double fromBaseTableCost);

    double getLocalCost();

    double getRemoteCost();

    double getLocalCostPerPartition();

    double getRemoteCostPerPartition();

    // Sets the value of localCostPerPartition directly.
    void setLocalCostPerPartition(double localCostPerPartition);

    // Sets the value of remoteCostPerPartition directly.
    void setRemoteCostPerPartition(double remoteCostPerPartition);

    // Sets localCostPerPartition as the total localCost divided by numPartitions.
    void setLocalCostPerPartition(double localCost, int numPartitions);

    // Sets remoteCostPerPartition as the total remoteCost divided by numPartitions.
    void setRemoteCostPerPartition(double remoteCost, int numPartitions);

    double getAccumulatedMemory();

    void setAccumulatedMemory(double memorySize);

    /**
     * @return True, if this is a single-row relation, otherwise false
     */
    public boolean isSingleRow();

    public void setSingleRow(boolean singleRowInRelation);
}
