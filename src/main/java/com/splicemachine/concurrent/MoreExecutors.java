package com.splicemachine.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Scott Fines
 *         Date: 8/5/14
 */
public class MoreExecutors {

    private MoreExecutors(){}

    public static ExecutorService namedSingleThreadExecutor(String nameFormat) {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
        return Executors.newSingleThreadExecutor(factory);
    }

}
