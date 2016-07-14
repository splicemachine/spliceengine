/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
