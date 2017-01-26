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