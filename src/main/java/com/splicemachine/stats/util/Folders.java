package com.splicemachine.stats.util;

/**
 * @author Scott Fines
 * Date: 1/24/14
 */
public class Folders {

		private static final LongLongFolder MAX_FOLDER = new LongLongFolder() {
				@Override
				public long fold(long previous, long next) {
						return previous >= next ? previous : next;
				}

				@Override
				public long foldAll(long initialValue, long... array) {
						long max = initialValue;
						for(int i=0;i<array.length;i++){
								max = fold(max,array[i]);
						}
						return max;
				}
		};

		private static final LongLongFolder MIN_FOLDER = new LongLongFolder() {
				@Override
				public long fold(long previous, long next) {
						return previous <= next ? previous : next;
				}
				@Override
				public long foldAll(long initialValue, long... array) {
						long min = initialValue;
						for(int i=0;i<array.length;i++){
								min = fold(min,array[i]);
						}
						return min;
				}
		};

		private static final LongLongFolder SUM_FOLDER = new LongLongFolder() {
				@Override
				public long fold(long previous, long next) {
						return previous+next;
				}

				@Override
				public long foldAll(long initialValue, long... array) {
						long sum = initialValue;
						for(int i=0;i<array.length;i++){
								sum = fold(sum,array[i]);
						}
						return sum;
				}
		};
		private static final DoubleFolder DOUBLE_MIN_FOLDER = new DoubleFolder() {
				@Override
				public double fold(double previous, double next) {
						return previous <= next ? previous : next;
				}
		};
		private static final DoubleFolder DOUBLE_MAX_FOLDER = new DoubleFolder() {
				@Override
				public double fold(double previous, double next) {
						return previous >= next ? previous : next;
				}
		};

		private Folders(){}

		public static LongLongFolder maxLongFolder(){
				return MAX_FOLDER;
		}
		public static LongLongFolder minLongFolder(){
				return MIN_FOLDER;
		}
		public static LongLongFolder sumFolder(){
				return SUM_FOLDER;
		}

		public static DoubleFolder minDoubleFolder(){
				return DOUBLE_MIN_FOLDER;
		}
		public static DoubleFolder maxDoubleFolder(){
				return DOUBLE_MAX_FOLDER;
		}
}
