package com.splicemachine.stats.histogram;

import com.carrotsearch.hppc.LongDoubleOpenHashMap;
import com.splicemachine.stats.order.IntMinMaxCollector;
import com.splicemachine.stats.IntUpdateable;

/**
 * Pulled from Cormode et al's "Fast Approximate Wavelet Tracking on Streams",
 * with help from Gilbert et al's "Surfing Wavelets on Streams: One-Pass Summaries for Approximate Aggregate Queries"
 * and Matias et al's "Wavelet-Based Histograms for Selectivity Estimation".
 *
 * @author Scott Fines
 * Date: 5/27/14
 */
public class IntGroupedCountBuilder implements IntUpdateable {
		private final ScalarGroupedCountSketch[] levels;
		private final double[] f;
		private final int lg;

		private long count;
		private double overallAverage;
		private final IntMinMaxCollector boundaryCollector;

		public static IntGroupedCountBuilder build(float epsilon, int maxDomainElement){
				int n = 2*maxDomainElement;
				int s = 1;
				int t =0;
				while(s<n){
						s<<=1;
						t++;
				}

				return new IntGroupedCountBuilder(epsilon,t,n);
		}

		public IntGroupedCountBuilder(float epsilon,int t,int expectedElements) {
				long s = 1;
				int lg = 0;
				while(s<expectedElements){
						s<<=1;
						lg++;
				}
				this.lg = lg;
				this.levels = new ScalarGroupedCountSketch[lg];
				this.f = new double[lg];
				for(int l=0;l<levels.length;l++){
						levels[l] = new ScalarGroupedCountSketch(t,epsilon);
						f[l] = 1d/Math.sqrt((1 << (lg-l)));
				}

				this.count = 0;
				this.overallAverage = 0d;

				this.boundaryCollector = IntMinMaxCollector.newInstance();
		}

		@Override
		public void update(int item) {
				update(item,1l);
		}

		public IntRangeQuerySolver build(double threshold){
				LongDoubleOpenHashMap coefs = findGreaterThan(threshold);
				return new IntWaveletQuerySolver(
								boundaryCollector.min(),
								boundaryCollector.max(),
								count, coefs,lg,f);
		}

		private LongDoubleOpenHashMap findGreaterThan(double phi) {
				double oa = ((double)count)/(1<<lg);
				double e = levels[0].estimateEnergy(0)+levels[1].estimateEnergy(0);
				LongDoubleOpenHashMap elements = new LongDoubleOpenHashMap(10);
				elements.put(0,oa);
				findGreaterThan(phi * e, 0, 0, elements);
				return elements;
		}

		private void findGreaterThan(double threshold,int level,int k,LongDoubleOpenHashMap elements){
				if(level>=levels.length) return;

				int shift = 1<<level;
				for(int g=k;g<k+2 && g<shift;g++){
						int group = shift+g;
						double energy = levels[level].estimateEnergy(g);
						if(energy>threshold){
								elements.put(group,levels[level].getValue(group));
								findGreaterThan(threshold, level + 1, 2 * g, elements);
						}
				}
		}

		@Override
		public void update(Integer item) {
				update(item,1l);
		}

		@Override
		public void update(Integer item, long count) {
				assert item!=null: "Cannot build a wavelet with null elements";
				update(item.intValue(),count);
		}

		@Override
		public void update(int item, long instanceCount) {
				this.boundaryCollector.update(item);

				float i = (float)item;
				for(int l=0;l<levels.length;l++){
						float kf = i/(1<<(lg-l));
						float sf = 1<<l;
						if(l==0)
								kf += 0.5f;
						else
								kf += (1<<(l-1));
						if(l==lg-1)
								sf += i;
						else
								sf += i/(1<<(lg-l-1));
						int k = (int)Math.floor(kf);
						boolean signum = ((int)Math.floor(sf)) %2 !=0;
						double vp = f[l]*instanceCount;
						if(signum) vp = -vp;

						levels[l].update((1<<l)+k,vp);
				}
				count+=instanceCount;
		}

		public static void main(String...args) throws Exception{
//				int[] a = new int[]{2,2,0,2,3,5,4,4};
//				IntGroupedCountBuilder builder = new IntGroupedCountBuilder(0.25f,4,16);
//				for(int elem:a){
//						builder.update(elem);
//				}
//
//				IntRangeQuerySolver querySolver = builder.build(0.0d);
//				for(int val=0;val<8;val++){
//						System.out.printf("equals: %d:%d%n",val,querySolver.equal(val));
//						System.out.printf("before: %d:%d%n",val,querySolver.before(val,false));
//						System.out.printf("after: %d:%d%n",val,querySolver.after(val,false));
//				}
//				System.out.println("--------");

				double d = Math.pow(Long.MAX_VALUE,2);
				System.out.println(d);
				System.out.println(Math.floor(d));
				double decimal = d-Math.floor(d);
				System.out.println(decimal);

		}

}
