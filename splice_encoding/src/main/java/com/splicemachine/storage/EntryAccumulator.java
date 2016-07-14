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

package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;

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
