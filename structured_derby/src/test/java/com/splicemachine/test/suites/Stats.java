package com.splicemachine.test.suites;

public final class Stats {
	private int count;
	private int sum;
	private int max;
	private int min;

	public Stats(){
		this.count = 0;
		this.sum = 0;
		this.max = Integer.MIN_VALUE;
		this.min = Integer.MAX_VALUE;
	}

	public int getSum(){ return this.sum;}

	public int getMax(){ return this.max;}

	public int getMin(){ return this.min;}

	public int getAvg(){ return this.count>0? this.sum/this.count:0;}

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
