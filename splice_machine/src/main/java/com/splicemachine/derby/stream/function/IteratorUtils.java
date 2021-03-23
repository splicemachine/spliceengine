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
 *
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.TaskContext;
import scala.collection.JavaConverters;

import java.util.Iterator;

public class IteratorUtils {
    public static <E> Iterator<E> asInterruptibleIterator(Iterator<E> it) {
        TaskContext context = TaskContext.get();
        if (context != null) {
            return (Iterator<E>) JavaConverters.asJavaIteratorConverter(new InterruptibleIterator(context, JavaConverters.asScalaIteratorConverter(it).asScala())).asJava();
        } else
            return it;
    }

    public static <E> Iterator<E> asInterruptibleIterator(TaskContext context, Iterator<E> it) {
        if (context != null) {
            return (Iterator<E>) JavaConverters.asJavaIteratorConverter(new InterruptibleIterator(context, JavaConverters.asScalaIteratorConverter(it).asScala())).asJava();
        } else
            return it;
    }
}
