package com.splicemachine.hpcc;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;

public class HPCCUtils {

	public static void clearAndNullObectArrayList(ObjectArrayList list) {
		list.clear();
		list = null;
	}

	public static void clearAndNullBitSet(BitSet bitset) {
		bitset.clear();
		bitset = null;
	}

}
