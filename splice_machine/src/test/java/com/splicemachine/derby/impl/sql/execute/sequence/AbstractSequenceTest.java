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

package com.splicemachine.derby.impl.sql.execute.sequence;

import java.io.IOException;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ArchitectureIndependent.class)
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
