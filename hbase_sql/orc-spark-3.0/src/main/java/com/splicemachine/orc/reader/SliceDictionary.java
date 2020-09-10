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
package com.splicemachine.orc.reader;

import io.airlift.slice.Slice;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;

import java.util.Arrays;


/**
 *
 * Using Parquet Dictionary for ORC based on Spark Implementation.
 *
 *
 */
public class SliceDictionary extends Dictionary {
    Slice[] slice;
    public SliceDictionary(Slice[] slice) {
        // Encoding Does not matter for us...
        super(null);
        this.slice = slice;
    }

    @Override
    public Encoding getEncoding() {
        return super.getEncoding();
    }

    @Override
    public Binary decodeToBinary(int id) {
        return Binary.fromConstantByteBuffer(slice[id].toByteBuffer());
    }


    @Override
    public int decodeToInt(int id) {
        return super.decodeToInt(id);
    }

    @Override
    public long decodeToLong(int id) {
        return super.decodeToLong(id);
    }

    @Override
    public float decodeToFloat(int id) {
        return super.decodeToFloat(id);
    }

    @Override
    public double decodeToDouble(int id) {
        return super.decodeToDouble(id);
    }

    @Override
    public boolean decodeToBoolean(int id) {
        return super.decodeToBoolean(id);
    }

    @Override
    public int getMaxId() {
        return 0;
    }

    public int size() {
        return slice.length;
    }

    @Override
    public boolean equals(Object obj) {
        assert obj != null;
        return Arrays.equals(slice,((SliceDictionary) obj).slice);
    }

    public boolean sliceEquals(Slice[] slice) {
        assert slice != null;
        return Arrays.equals(this.slice,slice);
    }

}
