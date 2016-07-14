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

package com.splicemachine.primitives;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Abstract representation of a Comparator for byte arrays.
 *
 * This adds an additional method which allows comparing only
 * a portion of the byte arrays (specified by an offset and a length).
 *
 * @author Scott Fines
 * Date: 10/7/14
 */
public interface ByteComparator extends Comparator<byte[]> {

    int compare(byte[] b1, int b1Offset,int b1Length, byte[] b2,int b2Offset,int b2Length);

    int compare(ByteBuffer buffer, byte[] b2,int b2Offset,int b2Length);

    int compare(ByteBuffer lBuffer, ByteBuffer rBuffer);

    boolean equals(byte[] b1, int b1Offset,int b1Length, byte[] b2,int b2Offset,int b2Length);

    boolean equals(byte[] b1, byte[] b2);

    boolean equals(ByteBuffer buffer, byte[] b2,int b2Offset,int b2Length);

    boolean equals(ByteBuffer lBuffer, ByteBuffer rBuffer);

    boolean isEmpty(byte[] stop);
}
