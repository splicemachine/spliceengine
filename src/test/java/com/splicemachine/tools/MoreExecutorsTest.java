package com.splicemachine.tools;

import com.splicemachine.concurrent.MoreExecutors;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class MoreExecutorsTest {

    @Test
    public void newSingleThreadExecutor_usesThreadWithExpectedName() throws Exception {

        ExecutorService executorService = MoreExecutors.namedSingleThreadExecutor("testName-%d");

        Future<String> threadName = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return Thread.currentThread().getName();
            }
        });

        assertTrue(threadName.get().matches("testName-\\d"));
        executorService.shutdown();
    }

}