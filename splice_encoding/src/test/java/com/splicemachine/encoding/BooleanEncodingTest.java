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
