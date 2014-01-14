package com.splicemachine.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Scott Fines
 *         Created on: 10/23/13
 */
public interface KeyedFuture<K,V> extends Future<V> {

    K getKey();
}
