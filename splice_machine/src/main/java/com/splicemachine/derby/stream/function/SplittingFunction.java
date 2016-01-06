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
