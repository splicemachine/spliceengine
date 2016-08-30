/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
