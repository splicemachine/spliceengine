package com.splicemachine.utils;

/**
 * Utilities for dealing with primitive integer arrays.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public class IntArrays {

		private IntArrays(){} //don't instantiate utility classes!

		public static int[] complement(int[] map,int size){
				int[] complement = count(size);
				for(int pos:map){
						complement[pos] = -1;
				}
				return complement;
		}
		public static int[] intersect(int[] map,int size){
			int[] intersect = negativeInitialize(size);
			for(int pos:map){
				intersect[pos] = pos;
			}
			return intersect;
		}
		
		private static int max(int[] map) {
				int max = Integer.MIN_VALUE;
				for (int aMap : map) {
						if (aMap > max)
								max = aMap;
				}
				return max;
		}

		public static int[] count(int size){
				int[] newInts = new int[size];
				for(int i=0;i<size;i++){
						newInts[i] = i;
				}
				return newInts;
		}
		public static int[] negativeInitialize(int size){
			int[] newInts = new int[size];
			for(int i=0;i<size;i++){
					newInts[i] = -1;
			}
			return newInts;
	}
}
