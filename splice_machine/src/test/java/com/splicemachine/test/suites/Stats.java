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
