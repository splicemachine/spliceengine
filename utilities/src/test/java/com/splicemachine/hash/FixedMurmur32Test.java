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

package com.splicemachine.hash;

import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 4/18/15
 */
public class FixedMurmur32Test{

    private final Murmur32 murmur32 = new Murmur32(0);
    @Test
    public void testIntSameAsByteArray() throws Exception {
        for(int i=0;i<3000;i++){
            byte[] bytes=Bytes.toBytes(i);
            int correct=murmur32.hash(bytes,0,bytes.length);

            int actual=murmur32.hash(i);

            Assert.assertEquals("Incorrect int hash!",correct,actual);
        }
    }
}
