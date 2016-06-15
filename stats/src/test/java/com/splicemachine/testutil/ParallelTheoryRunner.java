package com.splicemachine.testutil;

import org.junit.Assume;
import org.junit.experimental.theories.PotentialAssignment;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.experimental.theories.internal.Assignments;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.*;

import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * A Parallel Runner for Theories classes. This makes it so that different permutations
 * will be run in parallel in a (configured) threadpool. This means we can do more permutations
 * without significantly impacting our overall test times.
 * @author Scott Fines
 *         Date: 6/23/15
 */
public class ParallelTheoryRunner extends Theories{

    public ParallelTheoryRunner(Class<?> klass) throws InitializationError{
        super(klass);
        setScheduler(new ParallelScheduler());
    }

    @Override
    protected TestClass createTestClass(Class<?> testClass){
        return new com.splicemachine.testutil.CachedConstructorTestClass(testClass);
    }

    @Override
    public Statement methodBlock(FrameworkMethod method){
        return new ParallelTheoryAnchor(method,getTestClass());
    }

    private static class ParallelScheduler implements RunnerScheduler{
        private static final ForkJoinPool FORK_JOIN_POOL = setupForkJoinPool();

        private static ForkJoinPool setupForkJoinPool(){
            Runtime runtime = Runtime.getRuntime();
            int numThreads = runtime.availableProcessors();
            try{
                String configuredThreads = System.getProperty("maxParallelTheories");
                numThreads = Math.max(numThreads,Integer.parseInt(configuredThreads));
            }catch(Exception ignored){
                numThreads = 3*numThreads/4;
            }
            if(numThreads<1)
                numThreads = 1;
            ForkJoinPool.ForkJoinWorkerThreadFactory threadFactory = new ForkJoinPool.ForkJoinWorkerThreadFactory(){
                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool){
                    ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    thread.setName("TestTheoryRunner-"+thread.getName());
                    return thread;
                }
            };
            return new ForkJoinPool(numThreads,threadFactory,null,false);
        }

        private final Deque<ForkJoinTask<?>> tasks = new LinkedList<>();
        private Runnable lastScheduled;

        @Override
        public void schedule(Runnable childStatement){
            if(lastScheduled!=null){
                //submit the last child
                if(ForkJoinTask.inForkJoinPool()){
                    tasks.addFirst(ForkJoinTask.adapt(lastScheduled).fork());
                }else{
                    tasks.addFirst(FORK_JOIN_POOL.submit(lastScheduled));
                }
            }
            lastScheduled = childStatement;
        }

        @Override
        public void finished(){
            List<Throwable> errors = new LinkedList<>();
            if (lastScheduled != null) {
                if (ForkJoinTask.inForkJoinPool()) {
                    // Execute the last scheduled child in the current thread ...
                    try { lastScheduled.run(); } catch (Throwable t) { errors.add(t); }
                } else {
                    // Submit the last scheduled child to the ForkJoinPool too,
                    // because all tests should run in the worker threads ...
                    tasks.addFirst(FORK_JOIN_POOL.submit(lastScheduled));
                }
                // Make sure all asynchronously executed children are done, before we return ...
                for (ForkJoinTask<?> task : tasks) {
                    // Note: Because we have added all tasks via addFirst into _asyncTasks,
                    // task.join() is able to steal tasks from other worker threads,
                    // if there are tasks, which have not been started yet ...
                    // from other worker threads ...
                    try { task.join(); } catch (Throwable t) { errors.add(t); }
                }
                if(errors.size()>0)
                    throw new RuntimeException(new MultipleFailureException(Collections.unmodifiableList(errors)));
            }
        }
    }

    private class ParallelTheoryAnchor extends TheoryAnchor{
        private final Deque<ForkJoinTask<?>> runs = new LinkedBlockingDeque<>();
        private volatile boolean wasRunWithAssignmentCalled;

        private final FrameworkMethod testMethod;

        public ParallelTheoryAnchor(FrameworkMethod method,TestClass testClass){
            super(method,testClass);
            this.testMethod = method;
        }

        @Override
        protected void runWithAssignment(Assignments assignments) throws Throwable{
            if(wasRunWithAssignmentCalled)
                super.runWithAssignment(assignments);
            else{
                wasRunWithAssignmentCalled =true;
                super.runWithAssignment(assignments);
                /*
                 * Since this is the first time we entered this block,
                 * we need to make sure that all asynchronous runs have finished before we
                 * return.
                 */
                Throwable failure = null;
                while(failure==null &&!runs.isEmpty()){
                    ForkJoinTask<?> task = runs.removeFirst();
                    try{
                        task.join();
                    }catch(Throwable t){failure = t;}
                }
                if(failure!=null){
                    //cancel everything left, since we have an error
                    while(!runs.isEmpty()){
                        ForkJoinTask<?> task = runs.removeFirst();
                        try{
                            task.cancel(true);
                        }catch(Throwable ignored){}
                    }
                    //and join to make sure everything is happy
                    while(!runs.isEmpty()){
                        ForkJoinTask<?> task = runs.removeFirst();
                        try{
                            task.join();
                        }catch(Throwable ignored){}
                    }

                    throw failure;
                }
            }
        }

        @Override
        protected void runWithCompleteAssignment(final Assignments complete) throws Throwable{
            final TestClass clazz = getTestClass();
            new BlockJUnit4ClassRunner(getTestClass().getJavaClass()) {
                @Override
                protected TestClass createTestClass(Class<?> testClass){
                    return clazz;
                }

                @Override
                protected void collectInitializationErrors(
                    List<Throwable> errors) {
                    // do nothing
                }

                @Override
                public Statement methodBlock(final FrameworkMethod method) {
                    final Statement statement = super.methodBlock(method);
                    return new Statement() {
                        @Override
                        public void evaluate() throws Throwable {
                            try {
                                statement.evaluate();
                                handleDataPointSuccess();
                            } catch (AssumptionViolatedException e) {
                                handleAssumptionViolation(e);
                            } catch (Throwable e) {
                                reportParameterizedError(e, complete.getArgumentStrings(nullsOk()));
                            }
                        }

                    };
                }

                @Override
                protected Statement methodInvoker(FrameworkMethod method, Object test) {
                    return methodCompletesWithParameters(method, complete, test);
                }

                @Override
                public Object createTest() throws Exception {
                    Object[] params = complete.getConstructorArguments();

                    if (!nullsOk()) {
                        Assume.assumeNotNull(params);
                    }

                    return getTestClass().getOnlyConstructor().newInstance(params);
                }
            }.methodBlock(testMethod).evaluate();
        }



        //override to add synchronization
        @Override
        protected synchronized void handleAssumptionViolation(AssumptionViolatedException e){
            super.handleAssumptionViolation(e);
        }

        @Override
        protected synchronized void handleDataPointSuccess(){
            super.handleDataPointSuccess();
        }

        @Override
        protected void runWithIncompleteAssignment(Assignments incomplete) throws Throwable{
            for(PotentialAssignment source:incomplete.potentialsForNextUnassigned()){
                final Assignments nextAssignment = incomplete.assignNext(source);
                ForkJoinTask<?> run = new RecursiveAction(){
                    @Override
                    protected void compute(){
                        try{
                            ParallelTheoryAnchor.this.runWithAssignment(nextAssignment);
                        }catch(Throwable t){
                            throw new RuntimeException(t);
                        }
                    }
                };
                runs.addFirst(run.fork());
            }
        }

        private boolean nullsOk() {
            Theory annotation = testMethod.getMethod().getAnnotation(Theory.class);
            if (annotation == null) {
                return false;
            }
            return annotation.nullsAccepted();
        }

        private Statement methodCompletesWithParameters(
            final FrameworkMethod method, final Assignments complete, final Object freshInstance) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    final Object[] values = complete.getMethodArguments();

                    if (!nullsOk()) {
                        Assume.assumeNotNull(values);
                    }

                    method.invokeExplosively(freshInstance, values);
                }
            };
        }
    }


}
