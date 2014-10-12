package com.splicemachine.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.impl.sql.compile.CostEstimateImpl;
import org.apache.derby.impl.sql.compile.Level2CostEstimateImpl;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceCostEstimateImpl extends Level2CostEstimateImpl implements SortState {
    private static final Logger LOG = Logger.getLogger(SpliceCostEstimateImpl.class);
	protected int numberOfRegions;
	protected RowOrdering rowOrdering; 
	protected SpliceCostEstimateImpl baseCost;
	public SpliceCostEstimateImpl()  {
		SpliceLogUtils.trace(LOG, "spliceCostEstimate created");
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


	/** @see CostEstimate#cloneMe */
	public CostEstimate cloneMe() {
		return new SpliceCostEstimateImpl(cost,
									rowCount,
									singleScanRowCount,numberOfRegions,rowOrdering, baseCost);
	}

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
	
	/** @see CostEstimate#add */
	@Override
	public CostEstimate add(CostEstimate other, CostEstimate retval) {
		SpliceLogUtils.trace(LOG, "add other=%s, retval=%s",other,retval);
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(other instanceof CostEstimateImpl);
			SanityManager.ASSERT(retval == null ||
								retval instanceof CostEstimateImpl);
		}

		CostEstimateImpl	addend = (CostEstimateImpl) other;

		double sumCost = this.cost + addend.cost;
		double sumRowCount = this.rowCount + addend.rowCount;
		if (SanityManager.DEBUG)
		{
			if (sumCost < 0.0 || 
			    sumRowCount < 0.0)
			{
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
		return setState(sumCost,
						sumRowCount,
						(CostEstimateImpl) retval);
	}
	@Override
	/** @see CostEstimate#multiply */
	public CostEstimate multiply(double multiplicand, CostEstimate retval) {
		SpliceLogUtils.trace(LOG, "multiply multiplicand=%f, retval=%s",multiplicand,retval);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(retval == null ||
								retval instanceof CostEstimateImpl);
		}

		double multCost = this.cost * multiplicand;
		double multRowCount = this.rowCount * multiplicand;

		if (SanityManager.DEBUG)
		{
			if (multCost < 0.0 || 
			    multRowCount < 0.0)
			{
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
	/** @see CostEstimate#divide */
	public CostEstimate divide(double divisor, CostEstimate retval) {
		SpliceLogUtils.trace(LOG, "divide divide=%f, retval=%s",divisor,retval);
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(retval == null ||
								retval instanceof CostEstimateImpl);
		}

		double divCost = this.cost / divisor;
		double divRowCount = this.rowCount / divisor;

		if (SanityManager.DEBUG)
		{
			if (divCost < 0.0 || 
			    divRowCount < 0.0)
			{
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
		return setState(divCost,
						divRowCount,
						(CostEstimateImpl) retval);
	}

	@Override
	public void setCost(double cost, double rowCount, double singleScanRowCount) {
//		System.out.println("setCost " + cost);
//		Thread.dumpStack();
		SpliceLogUtils.trace(LOG, "setCost cost=%f, rowCount=%f, singleScanRowCount=%f",cost,rowCount,singleScanRowCount);
		super.setCost(cost, rowCount, singleScanRowCount);
		numberOfRegions=numberOfRegions==0?1:numberOfRegions;
	}

	@Override
	public void setCost(CostEstimate other) {
		SpliceLogUtils.trace(LOG, "setCost other=%s",other);
		cost = other.getEstimatedCost();
		rowCount = other.rowCount();
		singleScanRowCount = other.singleScanRowCount();
		numberOfRegions = ((SortState) other).getNumberOfRegions();
		rowOrdering = other.getRowOrdering();
		baseCost = ((SpliceCostEstimateImpl)other).baseCost;
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

	public int getNumberOfRegions() {
		return numberOfRegions;
	}

	public void setNumberOfRegions(int numberOfRegions) {
		SpliceLogUtils.trace(LOG, "setNumberOfRegions %d",numberOfRegions);
		this.numberOfRegions = numberOfRegions;
	}
	
	public RowOrdering getRowOrdering() {
		return rowOrdering;
	};
	
	public void setRowOrdering(RowOrdering rowOrdering) {
		this.rowOrdering = new SpliceRowOrderingImpl();
		if (rowOrdering!=null)
			rowOrdering.copy(this.rowOrdering); // Have to make a copy...
	}

	public SpliceCostEstimateImpl getBaseCost() {
		return baseCost==null?this:baseCost;
	}

	public void setBaseCost(SpliceCostEstimateImpl baseCost) {
//		System.out.println("setJoinCost " + baseCost);
//		Thread.dumpStack();
		this.baseCost = baseCost;
	};
	
	public void addBaseCost(double theCost,double theRowCount,double theSingleScanRowCount,int numberOfRegions, RowOrdering rowOrdering) {
		if (baseCost == null) {
			baseCost = new SpliceCostEstimateImpl(theCost,theRowCount,theSingleScanRowCount,numberOfRegions,rowOrdering);
		} else {
			baseCost.setCost(theCost, theRowCount, theSingleScanRowCount);
			baseCost.numberOfRegions = numberOfRegions;
			baseCost.rowOrdering = rowOrdering;
		}
//		System.out.println("addBaseCost " + baseCost);
//		Thread.dumpStack();
	}	
	
	
	@Override
	public CostEstimate getBase() {
		return getBaseCost();
	}

	@Override
	public void setBase(CostEstimate baseCost) {
		this.baseCost = (SpliceCostEstimateImpl) baseCost;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) return false;
		SpliceCostEstimateImpl comp = (SpliceCostEstimateImpl) obj;
		return (this.cost == comp.cost &&
				this.numberOfRegions == comp.numberOfRegions &&
				this.rowCount == comp.rowCount &&
				this.singleScanRowCount == comp.singleScanRowCount &&
				( (this.baseCost == null && comp.baseCost == null) ||
						this.baseCost == comp.baseCost)
				);
		// TODO Add Row Ordering
	}

}
