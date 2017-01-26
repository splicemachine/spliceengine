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

package com.splicemachine.hash;

import java.nio.ByteBuffer;

/**
 * Representation of a 64-bit hash function.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface Hash64 {

    long hash(String elem);

    long hash(byte[] bytes, int offset, int length);

    long hash(ByteBuffer byteBuffer);

    long hash(long element);

    long hash(int element);

    long hash(float element);

    long hash(double element);
}
