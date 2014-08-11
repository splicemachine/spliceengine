package com.splicemachine.stats.order;

import com.splicemachine.stats.IntUpdateable;

/**
 * @author Scott Fines
 * Date: 6/2/14
 */
public class IntMinMaxCollector implements MinMaxCollector<Integer>,IntUpdateable {
		private int currentMin;
		private int currentMax;

		@Override public void update(int item, long count) { update(item); }

		@Override
		public void update(int item) {
				if(currentMax<item)
						currentMax = item;
				if(currentMin>item)
						currentMin = item;
		}

		@Override
		public void update(Integer item) {
				assert item!=null: "Cannot order null elements";
				update(item.intValue());
		}

		@Override
		public void update(Integer item, long count) {
				update(item);
		}

		@Override public Integer minimum() { return currentMin; }
		@Override public Integer maximum() { return currentMax; }
		public int max(){ return currentMax; }
		public int min(){ return currentMin; }

		public static IntMinMaxCollector newInstance() {
				IntMinMaxCollector collector = new IntMinMaxCollector();
				collector.currentMin = Integer.MAX_VALUE;
				collector.currentMax = Integer.MIN_VALUE;
				return collector;
		}
}
