package com.splicemachine.test_tools;

import com.google.common.collect.Lists;

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