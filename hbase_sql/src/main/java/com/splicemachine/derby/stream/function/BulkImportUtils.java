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

package com.splicemachine.derby.stream.function;

import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by jyuan on 3/20/17.
 */
public class BulkImportUtils implements Serializable{
    public BulkImportUtils() {

    }
    public static Comparator<BulkImportPartition> getSearchComparator() {
        return new SearchComparator();
    }

    public static Comparator<Tuple2<Long, byte[]>> getSortDataComparator() {
        return new SortDataComparator();
    }

    public static Comparator<BulkImportPartition> getSortComparator() {
        return new SortComparator();
    }

    public static class SearchComparator extends SerializableComparator<BulkImportPartition> {
        public SearchComparator() {}

        @Override
        public int compare(BulkImportPartition o1, BulkImportPartition o2) {
            Long conglomerate1 = o1.getConglomerateId();
            Long conglomerate2 = o2.getConglomerateId();

            if (conglomerate1 > conglomerate2)
                return 1;
            else if (conglomerate1 < conglomerate2)
                return -1;
            else {
                byte[] key = o2.getStartKey();
                byte[] start = o1.getStartKey();
                byte[] end = o1.getEndKey();

                // both start key and end key are empty
                if ((start == null  || start.length == 0) &&
                        (end == null || end.length == 0)) {
                    return 0;
                }
                if (start == null  || start.length == 0) {
                    // start key is empty
                    if (Bytes.compareTo(end, key) <= 0)
                        return -1;
                    else
                        return 0;
                }
                else if (end == null || end.length == 0) {
                    // end key is empty
                    if (Bytes.compareTo(start, key) > 0)
                        return 1;
                    else
                        return 0;
                }
                if (Bytes.compareTo(start, key) <= 0 && Bytes.compareTo(end, key)>0)
                    return 0;
                else if (Bytes.compareTo(start, key) > 0)
                    return 1;
                else
                    return -1;

            }
        }
    }

    public static class SortComparator extends SerializableComparator<BulkImportPartition> {
        public SortComparator(){
        }

        @Override
        public int compare(BulkImportPartition o1, BulkImportPartition o2) {
            Long conglomerate1 = o1.getConglomerateId();
            Long conglomerate2 = o2.getConglomerateId();
            byte[] startKey1 = o1.getStartKey();
            byte[] startKey2 = o2.getStartKey();

            if (conglomerate1 > conglomerate2)
                return 1;
            else if (conglomerate1 < conglomerate2)
                return -1;
            else {
                // conglomerate1 == conglomerate2
                if ((startKey1 == null || startKey1.length == 0) &&
                        (startKey2 == null || startKey2.length ==0))
                    return 0;
                else if (startKey1 == null || startKey1.length == 0)
                    return -1;
                else if (startKey2 == null || startKey2.length ==0)
                    return 1;
                else
                    return Bytes.compareTo(startKey1, startKey2);
            }
        }
    }

    public static class SortDataComparator extends SerializableComparator<Tuple2<Long, byte[]>> {

        public SortDataComparator() {}

        @Override
        public int compare(Tuple2<Long, byte[]> o1, Tuple2<Long, byte[]> o2) {
            Long c1 = o1._1;
            Long c2 = o2._1;
            byte[] k1 = o1._2;
            byte[] k2 = o2._2;
            if (c1 < c2)
                return -1;
            else if (c1 > c2)
                return 1;
            else {
                return Bytes.compareTo(k1, k2);
            }
        }
    }
}
