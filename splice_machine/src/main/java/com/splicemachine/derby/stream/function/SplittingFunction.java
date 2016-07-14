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
