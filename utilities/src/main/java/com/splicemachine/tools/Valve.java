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

package com.splicemachine.tools;

/**
 * @author Scott Fines
 *         Date: 11/25/13
 */
public interface Valve {

		public static enum SizeSuggestion{
				INCREMENT{
						@Override
						public int adjust(int currentSize) {
								return currentSize+1;
						}
				},
				DECREMENT{
						@Override
						public int adjust(int currentSize) {
								return currentSize-1;
						}
				},
				HALVE{
						@Override
						public int adjust(int currentSize) {
								return currentSize/2;
						}
				},
				DOUBLE{
						@Override
						public int adjust(int currentSize) {
								return currentSize*2;
						}
				};

				public int adjust(int currentSize){ return currentSize;}
		}

		void adjustValve(SizeSuggestion suggestion);

		int tryAllow();

		void release();

		int getAvailable();

		void setMaxPermits(int newMax);
}
