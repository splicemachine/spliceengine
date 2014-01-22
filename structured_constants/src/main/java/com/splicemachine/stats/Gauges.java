package com.splicemachine.stats;

import com.splicemachine.stats.util.DoubleFolder;
import com.splicemachine.stats.util.FolderUtils;

/**
 * @author Scott Fines
 *         Date: 1/23/14
 */
public class Gauges {

		private Gauges(){}

		public static Gauge maxGauge(){
			return new FoldGauge(FolderUtils.maxDoubleFolder());
		}

		public static Gauge minGauge(){
				return new FoldGauge(FolderUtils.minDoubleFolder());
		}

		public static Gauge noOpGauge(){
				return NOOP_GAUGE;
		}

		private static final Gauge NOOP_GAUGE = new Gauge() {
				//no-op
				@Override public void update(double value) {  }
				@Override public double getValue() { return 0; }

				@Override public boolean isActive() { return false; }
		};

		private static class FoldGauge implements Gauge{
				private double current;
				private final DoubleFolder folder;

				private FoldGauge(DoubleFolder folder) {
						this.folder = folder;
				}

				@Override public void update(double value) {
					current = folder.fold(current,value);
				}

				@Override public double getValue() { return current; }
				@Override public boolean isActive() { return true; }
		}
}
