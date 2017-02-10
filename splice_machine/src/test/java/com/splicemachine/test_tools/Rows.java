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
