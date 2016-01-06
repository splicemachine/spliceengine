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
