/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.compile;

import org.spark_project.guava.primitives.Doubles;
import com.splicemachine.db.impl.sql.compile.RowOrderingImpl;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.impl.sql.compile.CostEstimateImpl;
import com.splicemachine.db.impl.sql.compile.Level2CostEstimateImpl;
import org.apache.log4j.Logger;

public class SpliceCostEstimateImpl extends Level2CostEstimateImpl implements SortState {

    private static final Logger LOG = Logger.getLogger(SpliceCostEstimateImpl.class);

    protected int numberOfRegions;
    protected RowOrdering rowOrdering;
    protected SpliceCostEstimateImpl baseCost;

    public SpliceCostEstimateImpl()  {
        SpliceLogUtils.trace(LOG, "splicecostestimate created");
    }

    public SpliceCostEstimateImpl(double theCost,double theRowCount,double theSingleScanRowCount)  {
        this(theCost, theRowCount, theSingleScanRowCount,1,null);
    }

    public SpliceCostEstimateImpl(double theCost,double theRowCount,double theSingleScanRowCount,int numberOfRegions, RowOrdering rowOrdering)  {
        super(theCost, theRowCount, theSingleScanRowCount);
        this.numberOfRegions = numberOfRegions;
        this.rowOrdering = rowOrdering;
        SpliceLogUtils.trace(LOG, "spliceCostEstimate created with cost=%f, rowCount=%f, singleScanRowCount=%f, numberOfRegions=%d, rowOrdering=%s"
                ,theCost,theRowCount,theSingleScanRowCount,numberOfRegions, rowOrdering);
    }

    public SpliceCostEstimateImpl(double theCost,double theRowCount,double theSingleScanRowCount,int numberOfRegions, RowOrdering rowOrdering, SpliceCostEstimateImpl baseCost)  {
        this(theCost,theRowCount,theSingleScanRowCount,numberOfRegions,rowOrdering);
        this.baseCost = baseCost;
    }

    /* Caution, not an exact clone, returned object will have null baseCost */
    @Override
    public SpliceCostEstimateImpl cloneMe() {
        RowOrdering clonedRowOrdering = new RowOrderingImpl();
        if(this.rowOrdering != null) {
            this.rowOrdering.copy(clonedRowOrdering);
        }
        return new SpliceCostEstimateImpl(cost,rowCount, singleScanRowCount,numberOfRegions, clonedRowOrdering, null);
    }

    @Override
    public String toString() {
        return "SpliceCostEstimateImpl: at " + hashCode() + ", cost == " + cost +
                ", rowCount == " + rowCount +
                ", singleScanRowCount == " + singleScanRowCount +
                ", numberOfRegions == " + numberOfRegions +
                ", rowOrdering == " + rowOrdering +
                ", baseCost == " + baseCost;
    }

    public CostEstimateImpl setState(double theCost,
                                     double theRowCount,
                                     CostEstimateImpl retval)  {
        SpliceLogUtils.trace(LOG,"setState cost=%f, rowCount=%f, retVal=%s",theCost, theRowCount, retval);
        if (retval == null)
            retval = new SpliceCostEstimateImpl();
        return super.setState(theCost, theRowCount, retval);
    }

    @Override
    public CostEstimate add(CostEstimate other, CostEstimate retval) {
        SpliceLogUtils.trace(LOG, "add other=%s, retval=%s",other,retval);
        assert other instanceof CostEstimateImpl;
        assert (retval == null || retval instanceof CostEstimateImpl);

        CostEstimateImpl addend = (CostEstimateImpl) other;

        double sumCost = this.cost + addend.cost;
        double sumRowCount = this.rowCount + addend.rowCount;
        if (SanityManager.DEBUG) {
            if (sumCost < 0.0 || sumRowCount < 0.0) {
                SanityManager.THROWASSERT(
                        "All sums expected to be < 0.0, " +
                                "\n\tthis.cost = " + this.cost +
                                "\n\taddend.cost = " + addend.cost +
                                "\n\tsumCost = " + sumCost +
                                "\n\tthis.rowCount = " + this.rowCount +
                                "\n\taddend.rowCount = " + addend.rowCount +
                                "\n\tsumRowCount = " + sumRowCount
                );
            }
        }

		/* Presume that ordering is not maintained */
        return setState(sumCost, sumRowCount, (CostEstimateImpl) retval);
    }

    @Override
    public CostEstimate multiply(double multiplicand, CostEstimate retval) {
        SpliceLogUtils.trace(LOG, "multiply multiplicand=%f, retval=%s",multiplicand,retval);

        assert (retval == null || retval instanceof CostEstimateImpl);

        double multCost = this.cost * multiplicand;
        double multRowCount = this.rowCount * multiplicand;

        if (SanityManager.DEBUG) {
            if (multCost < 0.0 || multRowCount < 0.0) {
                SanityManager.THROWASSERT(
                        "All products expected to be < 0.0, " +
                                "\n\tthis.cost = " + this.cost +
                                "\n\tmultiplicand = " + multiplicand +
                                "\n\tmultCost = " + multCost +
                                "\n\tthis.rowCount = " + this.rowCount +
                                "\n\tmultRowCount = " + multRowCount
                );
            }
        }

		/* Presume that ordering is not maintained */
        return setState(multCost,
                multRowCount,
                (CostEstimateImpl) retval);
    }

    @Override
    public CostEstimate divide(double divisor, CostEstimate retval) {
        SpliceLogUtils.trace(LOG, "divide divide=%f, retval=%s",divisor,retval);
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(retval == null ||
                    retval instanceof CostEstimateImpl);
        }

        double divCost = this.cost / divisor;
        double divRowCount = this.rowCount / divisor;

        if (SanityManager.DEBUG) {
            if (divCost < 0.0 || divRowCount < 0.0) {
                SanityManager.THROWASSERT(
                        "All products expected to be < 0.0, " +
                                "\n\tthis.cost = " + this.cost +
                                "\n\tdivisor = " + divisor +
                                "\n\tdivCost = " + divCost +
                                "\n\tthis.rowCount = " + this.rowCount +
                                "\n\tdivRowCount = " + divRowCount
                );
            }
        }

		/* Presume that ordering is not maintained */
        return setState(divCost, divRowCount, (CostEstimateImpl) retval);
    }

    @Override
    public void setCost(double cost, double rowCount, double singleScanRowCount, int numPartitions) {
        super.setCost(cost, rowCount, singleScanRowCount, numPartitions);
        this.numberOfRegions = numPartitions;
    }

    @Override
    public int partitionCount() {
        return numberOfRegions;
    }

    @Override
    public void setCost(CostEstimate other) {
        SpliceLogUtils.trace(LOG, "setCost other=%s",other);
        cost = other.getEstimatedCost();
        rowCount = other.rowCount();
        singleScanRowCount = other.singleScanRowCount();
        numberOfRegions = ((SortState) other).getNumberOfRegions();
        setRowOrdering(other.getRowOrdering());
        if(other instanceof SpliceCostEstimateImpl) {
            if(((SpliceCostEstimateImpl)other).baseCost != null) {
                baseCost = ((SpliceCostEstimateImpl)other).baseCost.cloneMe();
            }
        }
    }

    @Override
    public void setNumPartitions(int numPartitions){
        numberOfRegions = numPartitions;
    }

    @Override
    public void setSingleScanRowCount(double singleScanRowCount) {
        SpliceLogUtils.trace(LOG, "setSingleScanRowCount singleScanRowCount=%f",singleScanRowCount);
        super.setSingleScanRowCount(singleScanRowCount);
    }

    @Override
    public double getEstimatedCost() {
        return super.getEstimatedCost();
    }

    @Override
    public void setEstimatedCost(double cost) {
        super.setEstimatedCost(cost);
    }

    @Override
    public long getEstimatedRowCount() {
        return super.getEstimatedRowCount();
    }

    @Override
    public void setEstimatedRowCount(long count) {
        SpliceLogUtils.trace(LOG, "setEstimatedRowCount count=%d",count);
        super.setEstimatedRowCount(count);
    }

    @Override
    public int getNumberOfRegions() {
        return numberOfRegions;
    }

    @Override
    public void setNumberOfRegions(int numberOfRegions) {
        SpliceLogUtils.trace(LOG, "setNumberOfRegions %d",numberOfRegions);
        this.numberOfRegions = numberOfRegions;
    }

    @Override
    public RowOrdering getRowOrdering() {
        return rowOrdering;
    }

    @Override
    public void setRowOrdering(RowOrdering rowOrdering) {
        this.rowOrdering = new RowOrderingImpl();
        if (rowOrdering!=null)
            rowOrdering.copy(this.rowOrdering); // Have to make a copy...
    }

    @Override
    public CostEstimate getBase() {
        /* Returns this if baseCost is null, a bit unexpected perhaps */
        return baseCost==null?this:baseCost;
    }

    @Override
    public void setBase(CostEstimate baseCost) {
        this.baseCost = (SpliceCostEstimateImpl) baseCost;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SpliceCostEstimateImpl)) {
            return false;
        }
        SpliceCostEstimateImpl comp = (SpliceCostEstimateImpl) obj;
        return (Math.abs(this.cost - comp.cost) < 1e-9 &&
                this.numberOfRegions == comp.numberOfRegions &&
                Math.abs(this.rowCount - comp.rowCount) < 1e-9 &&
                Math.abs(this.singleScanRowCount - comp.singleScanRowCount) < 1e-9 &&
                ((this.baseCost == null && comp.baseCost == null) ||
                        this.baseCost != null && this.baseCost.equals(comp.baseCost))
        );
    }

    @Override
    public int hashCode(){
        int rC = 17;
        rC= 31*rC+Doubles.hashCode(cost);
        rC=31*rC+Doubles.hashCode(rowCount);
        rC=31*rC+numberOfRegions;
        rC=31*rC+Doubles.hashCode(singleScanRowCount);
        if(baseCost!=null && baseCost!=this)
            rC=31*rC+baseCost.hashCode();
        return rC;
    }
}
