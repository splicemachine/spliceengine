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
package com.splicemachine.derby.stream.function;

import com.splicemachine.primitives.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by jyuan on 3/26/17.
 */
public class HBaseBulkImportRowToSparkRowFunction implements Function<Tuple2<Long, Tuple2<byte[], byte[]>>, Row> {

    @Override
    public Row call(
            Tuple2<Long,Tuple2<byte[], byte[]>> t) throws Exception {

        Long conglomerateId = t._1;
        byte[] key = t._2._1;
        byte[] value = t._2._2;
        return RowFactory.create(conglomerateId, Bytes.toHex(key), value);
    }
}
