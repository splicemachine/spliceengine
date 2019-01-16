/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.types.SQLVarchar;
import org.apache.spark.api.java.function.Function;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.Row;

/**
 *
 * Allows a map to convert from RDD<ExecRow> to RDD<Row>
 *
 */
public class LocatedRowToRowAvroFunction implements Function<ExecRow, Row> {
    public LocatedRowToRowAvroFunction() {
        super();
    }
    @Override
    public Row call(ExecRow locatedRow) throws Exception {
        for(int i=0; i < locatedRow.size(); i++){
            if (locatedRow.getColumn(i + 1).getTypeName().equals("DATE")){
                if (locatedRow.getColumn(i + 1).isNull()){
                    locatedRow.setColumn(i + 1, new SQLVarchar());
                } else {
                    locatedRow.setColumn(i + 1, new SQLVarchar(locatedRow.getColumn(i + 1).getString().toCharArray()));
                }
            }
        }
        return (Row)locatedRow;
    }
}