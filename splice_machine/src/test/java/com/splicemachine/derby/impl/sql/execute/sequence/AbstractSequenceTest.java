package com.splicemachine.derby.impl.sql.execute.sequence;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class AbstractSequenceTest {
	@Test
	public void singleThreaded100BlockSingleIncrementTestWithRollover() throws Exception {
		Sequence sequence = new SpliceTestSequence(100,1,0);
		for (long i = 0; i< 1000; i++) {
			long next = sequence.getNext();
			Assert.assertTrue(i==next);
		}
	}
	
	@Test
	public void singleThreaded1BlockSingleIncrementTestWithRollover() throws Exception {
		Sequence sequence = new SpliceTestSequence(1,1,0);
		for (long i = 0; i< 1000; i++) {
			long next = sequence.getNext();
			Assert.assertTrue(i==next);
		}
	}
	
	@Test
	public void singleThreaded1BlockWithStarting20WithRollover() throws Exception {
		Sequence sequence = new SpliceTestSequence(1,1,20);
		for (long i = 0; i< 1000; i++) {
			long next = sequence.getNext();
			Assert.assertTrue(i+20==next);
		}
	}
	
	@Test
	public void singleThreaded100BlockWithStarting20Increment10WithRollover() throws Exception {
		Sequence sequence = new SpliceTestSequence(100,10,20);
		for (long i = 0; i< 1000; i++) {
			long next = sequence.getNext();
			Assert.assertTrue((i*10+20)==next);
		}
	}
	
	private class SpliceTestSequence extends AbstractSequence {
		long currentValue = -1;
		SpliceTestSequence(long blockAllocationSize, long incrementSteps, long startingValue) {
			super(blockAllocationSize,incrementSteps,startingValue);
		}
		
			@Override
			protected long getCurrentValue() throws IOException {
				if (currentValue == -1)
					return startingValue;
				return currentValue;
			}

			@Override
			protected boolean atomicIncrement(long nextValue) throws IOException {
				currentValue = nextValue;
				return true;
			}

			@Override
			public void close() throws IOException {				
			}
		}
			
}
