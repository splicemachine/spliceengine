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

package com.splicemachine.derby.stream.function;

import scala.Tuple2;

import java.io.Externalizable;

/**
 * Represents a function which takes in a single argument, and outputs a pair of elements. This exists mainly
 * to provide a Splice-specific api which can be wrapped out by different execution engines to provide architecture-
 * specific execution.
 *
 * In Spark, this is equivalent to {@code org.apache.spark.api.java.function.PairFunction<T,V1,V2>}
 *
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface SplittingFunction<T,V1,V2> extends Externalizable{

    Tuple2<V1,V2> call(T value) throws Exception;
}
