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
