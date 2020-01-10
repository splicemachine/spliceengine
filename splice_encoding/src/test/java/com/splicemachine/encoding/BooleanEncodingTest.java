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

package com.splicemachine.encoding;

import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 6/8/13
 */
public class BooleanEncodingTest {


    @Test
    public void testSerializesAndDeserializesCorrectly() throws Exception {
        byte[] trueBytes = ScalarEncoding.writeBoolean(true,false);
        boolean shouldBeTrue = ScalarEncoding.readBoolean(trueBytes,false);

        Assert.assertTrue("Incorrect serialization of true!",shouldBeTrue);

        byte[] falseBytes = ScalarEncoding.writeBoolean(false,false);
        boolean shouldBeFalse = ScalarEncoding.readBoolean(falseBytes,false);

        Assert.assertFalse("Incorrect serialization of false!", shouldBeFalse);

        byte[] trueBytesDesc = ScalarEncoding.writeBoolean(true,true);
        boolean shouldBeTrueDesc = ScalarEncoding.readBoolean(trueBytesDesc,true);

        Assert.assertTrue("Incorrect serialization of true!", shouldBeTrueDesc);

        byte[] falseBytesDesc = ScalarEncoding.writeBoolean(false,true);
        boolean shouldBeFalseDesc = ScalarEncoding.readBoolean(falseBytesDesc,true);

        Assert.assertFalse("Incorrect serialization of false!",shouldBeFalseDesc);
    }

    @Test
    public void testSortsCorrectly() throws Exception {
        byte[] trueBytes = ScalarEncoding.writeBoolean(true,false);
        byte[] falseBytes = ScalarEncoding.writeBoolean(false,false);

        Assert.assertTrue("Incorrect sort--false before true!", Bytes.BASE_COMPARATOR.compare(trueBytes, falseBytes)>0);
        trueBytes = ScalarEncoding.writeBoolean(true,true);
        falseBytes = ScalarEncoding.writeBoolean(false,true);

        Assert.assertTrue("Incorrect sort--false after true!", Bytes.BASE_COMPARATOR.compare(trueBytes,falseBytes)<0);
    }
}
