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
package com.splicemachine.derby.impl.storage;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.primitives.Bytes;
import scala.Tuple2;

/**
 * Created by jyuan on 2/6/18.
 */
public class KeyByRowIdFunction <Op extends SpliceOperation> extends SplicePairFunction<SpliceOperation,ExecRow,String,ExecRow> {

    @Override
    public String genKey(ExecRow row) {
        return Bytes.toHex(row.getKey());
    }

    public ExecRow genValue(ExecRow row) {
        return row.getClone();
    }

    @Override
    public Tuple2<String, ExecRow> call(ExecRow execRow) throws Exception {
        return new Tuple2(genKey(execRow), genValue(execRow));
    }
}
