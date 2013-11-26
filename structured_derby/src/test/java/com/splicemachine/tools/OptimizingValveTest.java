package com.splicemachine.tools;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 * Date: 11/25/13
 */
public class OptimizingValveTest {
		@Test
		public void testTryAllowAllowsOneThrough() throws Exception {
				FixedThreshold latencyThreshold = new FixedThreshold();
				latencyThreshold.position = OptimizingValve.Position.OK;
				Valve valve = new OptimizingValve(1,10, latencyThreshold,latencyThreshold);
				Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);
				//make sure we can't get another
				Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

				//release an entry
				valve.release();

				//make sure we can now acquire
				Assert.assertTrue("valve does not allow through entries after release!", valve.tryAllow() >= 0);
		}

		@Test
		public void testHighPositionClosesValve() throws Exception {
				FixedThreshold threshold = new FixedThreshold();
				threshold.position= OptimizingValve.Position.HIGH;

				OptimizingValve valve = new OptimizingValve(1,10,threshold,threshold);

				//try and get one
				Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);

				//make sure we can't get another
				Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

				//release
				valve.release();

				//close the valve
				valve.update(0d,0d);

				//make sure we can't get any
				Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);
		}

		@Test
		public void testLowPositionOpensValve() throws Exception {
				FixedThreshold threshold = new FixedThreshold();
				threshold.position= OptimizingValve.Position.LOW;

				OptimizingValve valve = new OptimizingValve(0,10,threshold,threshold);

				//make sure we can't get another
				Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

				//release an entry
				valve.release();

				//make sure we still can't get any
				Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

				valve.update(0d,0d);

				//try and get one
				Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);
		}

		private class FixedThreshold implements OptimizingValve.Threshold {
				private OptimizingValve.Position position;
				@Override
				public OptimizingValve.Position exceeds(double measure) {
						return position;
				}

				@Override
				public void reset() {
						//no-op
				}
		}
}
