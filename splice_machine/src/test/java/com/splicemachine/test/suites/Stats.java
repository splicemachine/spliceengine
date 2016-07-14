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

package com.splicemachine.test.suites;

public final class Stats {
	private int count;
	private long sum;
	private int max;
	private int min;

	public Stats(){
		this.count = 0;
		this.sum = 0;
		this.max = Integer.MIN_VALUE;
		this.min = Integer.MAX_VALUE;
	}

	public long getSum(){ return this.sum;}

	public int getMax(){ return this.max;}

	public int getMin(){ return this.min;}

	public int getAvg(){ return this.count>0? (int) (this.sum/this.count):0;}

	public int getCount(){ return this.count;}

	public void add(int next){
		this.count++;
		this.sum+=next;
		if(next > max)
			this.max = next;
		if(next < min)
			this.min = next;
	}

	@Override
	public String toString() {
		return "Stats{" +
				"count=" + count +
				", sum=" + sum +
				", max=" + max +
				", min=" + min +
				", avg=" + getAvg()+
				'}';
	}
}
