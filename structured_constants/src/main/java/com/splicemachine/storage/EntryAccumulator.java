package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public interface EntryAccumulator<T extends EntryAccumulator<T>> {

		void add(int position, byte[] data, int offset,int length);

		void addScalar(int position, byte[] data, int offset, int length);

		void addFloat(int position, byte[] data, int offset,int length);

		void addDouble(int position, byte[] data, int offset, int length);

		BitSet getRemainingFields();

		boolean isFinished();

    byte[] finish();

    void reset();

    boolean fieldsMatch(T oldKeyAccumulator);

    boolean hasField(int myFields);

//		ByteSlice getFieldSlice(int myField);

//		ByteSlice getField(int myField, boolean create);

		long getFinishCount();

		void markOccupiedScalar(int position);

		void markOccupiedFloat(int position);

		void markOccupiedDouble(int position);

		void markOccupiedUntyped(int position);

		boolean isInteresting(BitIndex potentialIndex);

		void complete();
}
