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

package com.splicemachine.test_tools;

import org.spark_project.guava.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Creates a table of integer rows where the value of a given cell = row*COLS+col.
 */
public class IntegerRows implements Iterable<Iterable<Object>> {

    private final int rowCount;
    private final int colCount;
    private final int rowStart;

    /**
     * Table starts with value 0 in first row, first col.
     */
    public IntegerRows(int rowCount, int colCount) {
        this(rowCount, colCount, 0);
    }

    /**
     * Table starts with specified startRow.
     */
    public IntegerRows(int rowCount, int colCount, int startRow) {
        this.rowCount = rowCount;
        this.colCount = colCount;
        this.rowStart = startRow;
    }

    @Override
    public Iterator<Iterable<Object>> iterator() {
        List rows = Lists.newArrayList();
        for (int row = rowStart; row < (rowStart + rowCount); row++) {
            ArrayList<Integer> rowList = Lists.newArrayListWithCapacity(colCount);
            for (int col = 0; col < colCount; col++) {
                rowList.add(row * colCount + col);
            }
            rows.add(rowList);
        }

        return rows.iterator();
    }

}