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

package com.splicemachine.ck.visitor;

import com.splicemachine.ck.decoder.UserDataDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

public class TableRowPrinter implements IRowPrinter {

    private final TableCellPrinter tableCellPrinter;

    public TableRowPrinter(UserDataDecoder decoder) {
        this.tableCellPrinter = new TableCellPrinter(decoder);
    }

    @Override
    public List<String> processRow(Result row) throws Exception {
        List<String> result = new ArrayList<>();
        for(Cell cell : row.listCells()) {
            tableCellPrinter.visit(cell);
        }
        result.add(tableCellPrinter.getOutput());
        return result;
    }
}
