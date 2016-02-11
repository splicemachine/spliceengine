package com.splicemachine.test_tools;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Create an array based row provider for use with TableCreator.
 */
public class Rows {

    private Rows() {
    }

    /**
     * Construct a single row.  Each object in the values array is a single column value.
     */
    public static Iterable<Object> row(Object... values) {
        return Lists.newArrayList(values);
    }

    /**
     * Construct a collection of rows.
     */
    public static <Object> Iterable<Iterable<Object>> rows(Iterable<Object>... rows) {
        return Lists.newArrayList(rows);
    }

    /**
     * Construct a collection of rows.
     */
    public static <Object> Iterable<Iterable<Object>> rows(List<Iterable<Object>> rows) {
        return Lists.newArrayList(rows);
    }
}
