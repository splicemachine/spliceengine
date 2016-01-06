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
