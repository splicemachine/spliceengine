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

package com.splicemachine.access.client;

import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class ClientRegionConstants{
    final static byte[] FLUSH = Bytes.toBytes("F");
    final static byte[] HOLD = Bytes.toBytes("H");
    public final static String SPLICE_SCAN_MEMSTORE_ONLY="MR";
    final public static String SPLICE_SCAN_MEMSTORE_PARTITION_BEGIN_KEY="PTBK";
    final public static String SPLICE_SCAN_MEMSTORE_PARTITION_END_KEY="PTEK";
    final public static String SPLICE_SCAN_MEMSTORE_PARTITION_SERVER="PTS";
    final public static KeyValue MEMSTORE_BEGIN = new KeyValue(HConstants.EMPTY_START_ROW,HOLD,HOLD,0l, new byte[0]);
//    final public static KeyValue MEMSTORE_END = new KeyValue(HConstants.EMPTY_END_ROW,HOLD,HOLD);
    final public static KeyValue MEMSTORE_BEGIN_FLUSH = new KeyValue(HConstants.EMPTY_START_ROW,FLUSH,FLUSH, 0l, new byte[0]);
}