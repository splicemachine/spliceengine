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
