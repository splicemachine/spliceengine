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
 * A splice-specific function interface. This should be wrapped out by the individual execution engines
 * in order to support the proper api for different architectures.
 *
 * In Spark-land, this is equivalent to {@code Function<T,V>}, but we use this in the API instead to avoid
 * the accidental hadoop dependency.
 *
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface ExternalizableFunction<T,V> extends Externalizable{

    V call(T value) throws Exception;
}
