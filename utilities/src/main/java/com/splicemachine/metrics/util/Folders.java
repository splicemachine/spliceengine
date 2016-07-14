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

package com.splicemachine.metrics.util;

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
