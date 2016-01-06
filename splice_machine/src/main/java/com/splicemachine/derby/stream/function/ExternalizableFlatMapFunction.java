package com.splicemachine.derby.stream.function;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface ExternalizableFlatMapFunction<T,V> extends Externalizable{

    Iterable<V> call(T t) throws Exception;
}
