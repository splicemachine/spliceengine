package com.splicemachine.tools;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Provides factory methods Similar to java.util.concurrent.Executors except generally taking thread name as an argument.
 */
public class SpliceExecutors {

    public static ExecutorService newSingleThreadExecutor(String nameFormat) {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
        return Executors.newSingleThreadExecutor(factory);
    }

}
