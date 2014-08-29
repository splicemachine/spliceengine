package com.splicemachine.test_tools;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Creates a table of integer rows where the value of a given cell f(row,col) = row*col+col.
 */
public class IntegerRows implements Iterable<Iterable<Object>> {

    private final int rowCount;
    private final int colCount;

    public IntegerRows(int rowCount, int cols) {
        this.rowCount = rowCount;
        this.colCount = cols;
    }

    @Override
    public Iterator<Iterable<Object>> iterator() {
        List<List<Integer>> rows = Lists.newArrayList();
        for (int row = 0; row < rowCount; row++) {
            ArrayList<Integer> rowList = Lists.newArrayListWithCapacity(colCount);
            for (int col = 0; col < colCount; col++) {
                rowList.add(row * colCount + col);
            }
            rows.add(rowList);
        }

        return new X(rows);
    }

    /* Trick compiler.  Get rid of this if possible */
    private static class X implements Iterator<Iterable<Object>> {

        List<List<Integer>> rows;
        int index;

        private X(List<List<Integer>> rows) {
            this.rows = rows;
        }

        @Override
        public boolean hasNext() {
            return index < rows.size();
        }

        @Override
        public Iterable<Object> next() {
            return (List) rows.get(index++);
        }

        @Override
        public void remove() {

        }
    }

}
