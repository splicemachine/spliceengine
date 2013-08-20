package com.splicemachine.utils;

import com.google.common.collect.Lists;
import org.junit.*;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Created on: 7/25/13
 */
public class ConcurrentRingBufferTest {
    private static final int numThreads = 4;
    private static final int numIterations = 100000;

    private static ExecutorService executor;

    private List<Future<Boolean>> futures;

    @BeforeClass
    public static void setUpClass() throws Exception {
       executor = Executors.newFixedThreadPool(numThreads);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
       executor.shutdownNow();
    }

    @Before
    public void setUp() throws Exception {
        futures = Lists.newArrayListWithCapacity(numThreads);
    }

    @After
    public void tearDown() throws Exception {
        //make sure all futures complete
//        for(Future future:futures){
//            try{
//                future.get();
//            }catch(Exception e){
//                e.printStackTrace();
//            }
//        }
    }

    @Test
    public void testWorksWithSingleReader() throws Exception {
        ConcurrentRingBuffer<String> buffer = new ConcurrentRingBuffer<String>(1,new String[1],new ConcurrentRingBuffer.Filler<String>() {
            @Override
            public void prepareToFill() throws ExecutionException {
               //no-op
            }

            @Override
            public String getNext(String old) throws ExecutionException {
                return "testString";
            }

            @Override
            public void finishFill() throws ExecutionException {
                //no-op
            }
        });

        Assert.assertEquals("Incorrect return type!","testString",buffer.next());
    }

    @Test
    public void testWorksWithManyReaders() throws Throwable {
        final ConcurrentSkipListMap<Integer,Boolean> presenceMap = new ConcurrentSkipListMap<Integer, Boolean>();
        final AtomicInteger counter = new AtomicInteger(0);
        final ConcurrentRingBuffer.Filler<Integer> filler = new ConcurrentRingBuffer.Filler<Integer>() {

            @Override
            public void prepareToFill() throws ExecutionException {
                //no-op
            }

            @Override
            public Integer getNext(Integer old) throws ExecutionException {
                return counter.getAndIncrement();
            }

            @Override
            public void finishFill() throws ExecutionException {
                //no-op;
            }
        };

        final int bufferSize =10;
        final ConcurrentRingBuffer<Integer> buffer = new ConcurrentRingBuffer<Integer>(bufferSize,new Integer[bufferSize],filler);
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    for(int i=0;i<numIterations;i++){
                        try{
                            Integer next = buffer.next();
                            Assert.assertTrue("Received an unexpected null entry!",next!=null);
//                            if(next%1000==0)
//                                System.out.println(next);

                            Boolean aBoolean = presenceMap.putIfAbsent(next, Boolean.TRUE);
                            if(aBoolean!=null&&aBoolean){
                                Assert.fail("Entry already exists! Entry = "+ next+ ", highest = "+ presenceMap.lastKey());
                            }
                        }catch(Throwable t){
                            throw new Exception(t);
                        }
                    }
                    return true;
                }
            }));
        }

        for(Future<Boolean> future:futures){
            future.get();
        }
        if(presenceMap.size()>0)
            System.out.println(presenceMap.lastKey());

        //make sure that every number between 1 and numIterations*numThreads has been caught
        for(int i=0;i<numIterations*numThreads;i++){
            Assert.assertTrue("Entry "+ i+" is missing from presenceMap",presenceMap.containsKey(i));
        }
    }

    @Test
    @Ignore("Unnecessary, and takes a long time. Unignore if something breaks")
    public void testRepeatedMultiThreadedCheck() throws Throwable {
        for(int i=0;i<100;i++){
            System.out.println(i);
            testWorksWithManyReaders();
        }
    }
}
