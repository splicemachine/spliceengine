/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
