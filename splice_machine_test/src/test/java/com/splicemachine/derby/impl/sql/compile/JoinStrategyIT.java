package com.splicemachine.derby.impl.sql.compile;

import org.junit.Assert;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.impl.sql.compile.BaseJoinStrategy;
import org.junit.Test;

import com.splicemachine.constants.SpliceConstants;

public class JoinStrategyIT {
	public static SpliceRowOrderingImpl  RO1;
	public static SpliceRowOrderingImpl  RO2;
	public static SpliceRowOrderingImpl  RO3;
	public static SpliceRowOrderingImpl  RO4;
	public static SpliceRowOrderingImpl  RO5;
	public static SpliceRowOrderingImpl  RO6;
	public static SpliceCostEstimateImpl LT1; 
	public static SpliceCostEstimateImpl LT2; 
	public static SpliceCostEstimateImpl ST1; 
	public static SpliceCostEstimateImpl ST2;
	public static SpliceCostEstimateImpl OT1; 
	public static SpliceCostEstimateImpl OT2;
	
	public enum JoinType {
		NLJ (new NestedLoopJoinStrategy()), 
		MJ (new MergeJoinStrategy()), 
		BJ (new BroadcastJoinStrategy()), 
		MSJ (new MergeSortJoinStrategy());
		
		private final BaseJoinStrategy joinStrategy;
		
		JoinType(BaseJoinStrategy joinStrategy) {
			this.joinStrategy = joinStrategy;
		}

		public BaseJoinStrategy getStrategy() {
			return joinStrategy;
		}
		
		public double calc(CostEstimate outer, CostEstimate inner) {
			return cost(outer,inner).getEstimatedCost();
		}

		public CostEstimate cost(CostEstimate outer, CostEstimate inner) {
			joinStrategy.rightResultSetCostEstimate(null,outer,inner);
			return inner;
		}

	}
	
	public static CostEstimate LT1() {
		return LT1.cloneMe();
	}

	public static CostEstimate LT2() {
		return LT2.cloneMe();
	}
	public static CostEstimate ST1() {
		return ST1.cloneMe();
	}
	public static CostEstimate ST2() {
		return ST2.cloneMe();
	}
	public static CostEstimate OT1() {
		return OT1.cloneMe();
	}
	public static CostEstimate OT2() {
		return OT2.cloneMe();
	}

	public static double LT1ROWS = 10000000;
	public static double LT2ROWS = 20000000;
	public static double ST1ROWS = 100;
	public static double ST2ROWS = 200;
	public static double OTROWS = 1;

	
	static {
		RO1 = new SpliceRowOrderingImpl();
		RO1.addOrderedColumn(1, 1, 1);
		RO2 = new SpliceRowOrderingImpl();
		RO2.addOrderedColumn(2, 1, 1);
		RO3 = new SpliceRowOrderingImpl();
		RO3.addOrderedColumn(3, 1, 1);
		RO4 = new SpliceRowOrderingImpl();
		RO4.addOrderedColumn(4, 1, 1);
		LT1 = new SpliceCostEstimateImpl(LT1ROWS,LT1ROWS,LT1ROWS,10,RO1); 
		LT2 = new SpliceCostEstimateImpl(LT2ROWS,LT2ROWS,LT2ROWS,20,RO2); 
		ST1 = new SpliceCostEstimateImpl(ST1ROWS,ST1ROWS,ST1ROWS,1,RO3); 
		ST2 = new SpliceCostEstimateImpl(ST2ROWS,ST2ROWS,ST2ROWS,2,RO4); 
		OT1 = new SpliceCostEstimateImpl(OTROWS,OTROWS,OTROWS,1,RO5); 
		OT2 = new SpliceCostEstimateImpl(OTROWS,OTROWS,OTROWS,1,RO6); 
	}
	
	@Test
	public void testNLJ() throws StandardException {
			SpliceCostEstimateImpl left = (SpliceCostEstimateImpl) LT1();
			SpliceCostEstimateImpl right = (SpliceCostEstimateImpl) LT2();			
			JoinType.NLJ.cost(left,right);
			// Do not touch the left side
			Assert.assertEquals("Cannot modify the cost of the left side",left, LT1());
			// Test the join metrics
			Assert.assertEquals("Join Number of Regions is not moved the right",right.numberOfRegions,LT1.numberOfRegions);
			Assert.assertEquals("Join Single Scan Count Issue",LT1ROWS*LT2ROWS,right.singleScanRowCount(),0.1);	
			Assert.assertEquals("Join Estimated Row Count Issue",LT1ROWS*LT2ROWS,right.rowCount(),0.1);	
			Assert.assertEquals("Join Cost wrong",LT1ROWS+(LT1ROWS*LT1ROWS*LT2ROWS*SpliceConstants.optimizerNetworkCost)/10,right.getEstimatedCost(),0.1);
			Assert.assertTrue("Join Ordering Not Moved Left to Right",right.rowOrdering.orderedOnColumn(1, 1, 1));			
			// Test the base metrics
			Assert.assertEquals("Base Number of Regions Changed",20,right.baseCost.numberOfRegions,0.1);	
			Assert.assertEquals("Base Single Scan Count Changed",LT2ROWS,right.baseCost.singleScanRowCount,0.1);	
			Assert.assertEquals("Base Estimated Row Count Issue",LT1ROWS*LT2ROWS,right.baseCost.rowCount,0.1);	
			Assert.assertEquals("Base Right Side Cost Issue",(LT1ROWS*LT1ROWS*LT2ROWS*SpliceConstants.optimizerNetworkCost)/10,right.baseCost.cost,0.1);	
			Assert.assertTrue("Base Row Ordering Unchanged",right.baseCost.rowOrdering.orderedOnColumn(2, 1, 1));			
		}
			
	@Test
	public void testLargeTwoWayJoin() {
		Assert.assertTrue("NLJ is less expensive than MSJ",JoinType.NLJ.calc(LT1(),LT2()) > JoinType.MSJ.calc(LT1(),LT2()));
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(LT1(),LT2()) > JoinType.BJ.calc(LT1(),LT2()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(LT1(),LT2()) > JoinType.MJ.calc(LT1(),LT2()));
	}

	@Test
	public void testLargeSmallTwoWayJoin() {
		Assert.assertTrue("NLJ is less expensive than MSJ",JoinType.NLJ.calc(LT1(),ST1()) > JoinType.MSJ.calc(LT1(),ST1()));
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(LT1(),ST1()) > JoinType.BJ.calc(LT1(),ST1()));		
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(LT1(),ST1()) > JoinType.MJ.calc(LT1(),ST1()));
	}

	@Test
	public void testLargeIndexedLargeTwoWayJoin() {
		Assert.assertTrue(String.format("NLJ[%s] is less expensive than MSJ[%s]",JoinType.NLJ.calc(LT1(),OT1()),JoinType.MSJ.calc(LT1(),LT1())),JoinType.NLJ.calc(LT1(),OT1()) > JoinType.MSJ.calc(LT1(),LT1()));
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(LT1(),LT1()) > JoinType.BJ.calc(LT1(),LT1()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(LT1(),LT1()) > JoinType.MJ.calc(LT1(),LT1()));
	}

	@Test
	public void testIndexLargeLargeTwoWayJoin() {
		Assert.assertTrue(String.format("NLJ[%s] is less expensive than MSJ[%s]",JoinType.NLJ.calc(OT1(),LT1()),JoinType.MSJ.calc(LT1(),LT1())),JoinType.NLJ.calc(LT1(),OT1()) > JoinType.MSJ.calc(LT1(),LT1()));
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(OT1(),LT1()) > JoinType.BJ.calc(OT1(),LT1()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(OT1(),LT1()) > JoinType.MJ.calc(OT1(),LT1()));
	}

	@Test
	public void testIndexLargeSmallTwoWayJoin() {
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(OT1(),ST1()) > JoinType.NLJ.calc(OT1(),ST1()));
		Assert.assertTrue(String.format("NLJ[%s] is less expensive than BJ[%s]",JoinType.NLJ.calc(OT1(),ST1()),JoinType.BJ.calc(OT1(),ST1())),JoinType.NLJ.calc(OT1(),ST1()) > JoinType.BJ.calc(OT1(),ST1()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(OT1(),ST1()) > JoinType.MJ.calc(OT1(),ST1()));
	}

	@Test
	public void testSingleLargeTwoWayJoin() {
		Assert.assertTrue("NLJ is less expensive than MSJ",JoinType.NLJ.calc(OT1(),LT1()) > JoinType.MSJ.calc(OT1(),LT1()));
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(OT1(),LT1()) > JoinType.BJ.calc(OT1(),LT1()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(OT1(),LT1()) > JoinType.MJ.calc(OT1(),LT1()));
	}

	@Test
	public void testSingleSmallTwoWayJoin() {
		Assert.assertTrue("MSJ is less expensive than NLJ",JoinType.MSJ.calc(OT1(),ST1()) > JoinType.NLJ.calc(OT1(),ST1()));
		Assert.assertTrue("NLJ is less expensive than BJ",JoinType.NLJ.calc(OT1(),ST1()) > JoinType.BJ.calc(OT1(),ST1()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(OT1(),ST1()) > JoinType.MJ.calc(OT1(),ST1()));
	}
	
	@Test
	public void testSmallTwoWayJoin() {
		Assert.assertTrue("NLJ is less expensive than MSJ",JoinType.NLJ.calc(ST1(),ST2()) > JoinType.MSJ.calc(ST1(),ST2()));
		Assert.assertTrue("MSJ is less expensive than BJ",JoinType.MSJ.calc(ST1(),ST2()) > JoinType.BJ.calc(ST1(),ST2()));
		Assert.assertTrue("BJ is less expensive than MJ",JoinType.BJ.calc(ST1(),ST2()) > JoinType.MJ.calc(ST1(),ST2()));
	}
	
}
