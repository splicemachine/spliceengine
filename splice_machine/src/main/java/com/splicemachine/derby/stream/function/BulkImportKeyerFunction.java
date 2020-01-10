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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import scala.Tuple2;


public class BulkImportKeyerFunction extends
        SpliceFunction <SpliceOperation, Tuple2<Long, Tuple2<byte[], byte[]>>, Tuple2<Long, byte[]>> {

    public Tuple2<Long, byte[]> call(Tuple2<Long, Tuple2<byte[], byte[]>> row) throws Exception {
        Long conglomerateId = row._1;
        Tuple2<byte[], byte[]> kvPair = row._2;
        byte[] rowKey = kvPair._1;
        return new Tuple2<>(conglomerateId, rowKey);
    }
}
