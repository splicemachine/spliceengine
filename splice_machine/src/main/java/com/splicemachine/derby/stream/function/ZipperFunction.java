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

import java.io.Externalizable;

/**
 * An interface to represent a function which takes two arguments, and outputs a single argument. This is
 * to provide a Splice-specific implementation which can then be wrapped by different execution engines to provide
 * architecture-specific behavior.
 *
 * In Spark, this is equivalent to {@code org.apache.spark.api.java.function.Function2<T1,T2,V>}.
 *
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface ZipperFunction<T1,T2,V> extends Externalizable{

    V call(T1 first,T2 second) throws Exception;
}
