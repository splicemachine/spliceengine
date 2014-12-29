package com.splicemachine.test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.metrics.DistributionTimeView;
import com.splicemachine.metrics.LatencyTimer;
import com.splicemachine.metrics.LatencyView;
import com.splicemachine.metrics.Metrics;
import org.apache.commons.cli.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 7/21/14
 */
public abstract class BaseQueryRunner {

    protected Options getOptions(){
        Options options = new Options();

        Option threadCount = new Option("t","threads",true,"The number of threads to submit. Default is 16");
        threadCount.setRequired(false);
        options.addOption(threadCount);
        Option iterationCount = new Option("i","iterations",true,"The number of iterations to submit. Default is 100");
        iterationCount.setRequired(false);
        options.addOption(iterationCount);

        Option outputDirectory = new Option("o","output",true,"The Directory to log output to. " +
                "Default is current working directory. Each thread will have its own file");
        outputDirectory.setRequired(false);
        options.addOption(outputDirectory);

        Option jdbcHost = new Option("h","host", true, "The JDBC host to connect to. Default is localhost");
        jdbcHost.setRequired(false);
        options.addOption(jdbcHost);

        Option jdbcPort = new Option("p","port",true,"The JDBC port to connect to. Default is 1527.");
        jdbcPort.setRequired(false);
        options.addOption(jdbcPort);

        return options;
    }

    protected abstract Runner newRunner(Connection jdbcConn,
                               int numIterations,
                               File outputDir,
                               int threadId,
                               CountDownLatch startLatch) throws IOException;

    protected abstract void parseAdditionalOptions(CommandLine cli) throws Exception;

    public int run(String... args) throws Exception{
        Options options = getOptions();
        CommandLineParser parser = new GnuParser();

        CommandLine cli = parser.parse(options,args);

        int threads = Integer.parseInt(cli.getOptionValue("t", "16"));

        String outputDirStr = cli.getOptionValue("o",".");
        File outputDir = new File(outputDirStr);
        if(!outputDir.exists())
            throw new IllegalArgumentException("Output directory "+ outputDirStr+" does not exist!");
        if(!outputDir.isDirectory())
            throw new IllegalArgumentException("Output directory "+ outputDirStr+" is not a directory!");

        String host = cli.getOptionValue("h","localhost");
        String port = cli.getOptionValue("p","1527");

        int iterations = Integer.parseInt(cli.getOptionValue("i","1000"));

        parseAdditionalOptions(cli);

        String connectString = formatConnectString(host,port);

        execute(threads, outputDir, iterations, connectString);

        return 0;
    }

    protected void execute(int threads, File outputDir, int iterations, String connectString) throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        CompletionService<RunStats> completionService = new ExecutorCompletionService<>(threadPool);
        List<Connection> connections = Lists.newArrayListWithCapacity(threads);
        try{

            CountDownLatch startLatch = new CountDownLatch(1);
            for(int i=0;i<threads;i++){
                Connection conn = getConnection(connectString);
                connections.add(conn);
                completionService.submit(newRunner(conn,iterations,outputDir,i,startLatch));
            }

            //everyone has been submitted, let them all start running
            long start = System.nanoTime();
            startLatch.countDown();

            //wait for everyone to complete
            List<RunStats> stats = Lists.newArrayList();
            for(int i=0;i<threads;i++){
                Future<RunStats> take = completionService.take();
                RunStats runStats = take.get();
                runStats.print();
                stats.add(runStats);
                System.out.println("----");
            }
            long stop = System.nanoTime();
            double totalElapsedTime = (stop - start) / NANOS_TO_SECONDS;

            reportStats(threads, iterations, stats, totalElapsedTime);
        }finally{
            for(Connection con:connections){
                con.close();
            }
            threadPool.shutdownNow();
        }
    }

    private void reportStats(int threads, int iterations, List<RunStats> stats, double totalElapsedTime) {
        System.out.println("-----------");
        System.out.println("Stats:");
        long totalQueries = iterations*threads;
        System.out.printf("Total Time taken(s):%f%n", totalElapsedTime);
        System.out.printf("Total Queries/s:%d%n", (long)(totalQueries/totalElapsedTime));

        double maxAvgLatency = 0d;
        long maxMedianLatency = 0;
        long max75pLatency = 0;
        long max90pLatency = 0;
        long max95pLatency = 0;
        long max99pLatency = 0;
        long absMinLatency = Long.MAX_VALUE;
        long absMaxLatency = 0;
        long max25pLatency = 0;
        for(RunStats stat:stats){
            LatencyView runTimeData = stat.runTimeData.wallLatency();

            long minLat = runTimeData.getMinLatency();
            if(minLat<absMinLatency)
                absMinLatency = minLat;
            long maxLat = runTimeData.getMaxLatency();
            if(maxLat>absMaxLatency)
                absMaxLatency = maxLat;

            double avg = runTimeData.getOverallLatency();
            if(avg>maxAvgLatency)
                maxAvgLatency = avg;
            long p25 = runTimeData.getP25Latency();
            if(p25>max25pLatency)
                max25pLatency = p25;
            long med = runTimeData.getP50Latency();
            if(med>maxMedianLatency)
                maxMedianLatency = med;
            long p75 = runTimeData.getP75Latency();
            if(p75>max75pLatency)
                max75pLatency = p75;
            long p90 = runTimeData.getP90Latency();
            if(p90>max90pLatency)
                max90pLatency = p90;
            long p95 = runTimeData.getP95Latency();
            if(p95>max95pLatency)
                max95pLatency = p95;
            long p99 = runTimeData.getP99Latency();
            if(p99>max99pLatency)
                max99pLatency = p99;
        }
        System.out.printf("Overall Avg. Latency(ms):%f%n",maxAvgLatency/NANOS_TO_MILLIS);
        System.out.printf("Min Latency(ms):%f%n",absMinLatency/NANOS_TO_MILLIS);
        System.out.printf("Overall 25%% Latency(ms):%f%n",max25pLatency/NANOS_TO_MILLIS);
        System.out.printf("Overall 50%% Latency(ms):%f%n",maxMedianLatency/NANOS_TO_MILLIS);
        System.out.printf("Overall 75%% Latency(ms):%f%n",max75pLatency/NANOS_TO_MILLIS);
        System.out.printf("Overall 90%% Latency(ms):%f%n",max90pLatency/NANOS_TO_MILLIS);
        System.out.printf("Overall 95%% Latency(ms):%f%n",max95pLatency/NANOS_TO_MILLIS);
        System.out.printf("Overall 99%% Latency(ms):%f%n",max99pLatency/NANOS_TO_MILLIS);
        System.out.printf("Max Latency(ms):%f%n",absMaxLatency/NANOS_TO_MILLIS);
    }

    private static Connection getConnection(String connectString) throws Exception {
        Class.forName("org.apache.derby.jdbc.ClientDriver");
        return DriverManager.getConnection(connectString);
    }

    private static String formatConnectString(String host, String port) {
        return "jdbc:derby://"+host+":"+port+"/splicedb;user=splice;password=admin";
    }

    protected abstract class Runner implements Callable<RunStats> {
        protected final Connection connection;
        protected final int threadId;
        private final BufferedWriter outputWriter;
        private final int iterations;
        private final CountDownLatch startLatch;

        protected Runner(Connection connection,
                       int numIterations,
                       File outputDirectory,
                       int threadId,
                       CountDownLatch startLatch) throws IOException {
            this.connection = connection;
            this.threadId = threadId;

            File outputFile = new File(outputDirectory,"concurrent-runner-"+Integer.toString(threadId));
            if(outputFile.exists())
                outputFile.delete();
            outputFile.createNewFile();
            this.outputWriter = new BufferedWriter(new FileWriter(outputFile));
            this.iterations = numIterations;
            this.startLatch = startLatch;
        }

        @Override
        public RunStats call() throws Exception {
            startLatch.await();
            try (PreparedStatement ps = getPreparedStatement()) {
                int sampleSize;
                if (iterations < 100)
                    sampleSize = iterations;
                else if (iterations < 10000)
                    sampleSize = iterations / 10; //store 10% of the iterations up to 10000
                else
                    sampleSize = iterations / 100; //store 1% of iterations after that
                LatencyTimer timer = Metrics.sampledLatencyTimer(sampleSize);
                int numErrors = 0;
                int onePercentIter = iterations / 10;
                int s = 1;
                while (s < onePercentIter) {
                    s <<= 1;
                }
                for (int i = 0; i < iterations; i++) {
                    timer.startTiming();
                    try {
                        executeIteration(i, ps);
                    } catch (SQLException se) {
                        numErrors++;
                        reportError(i, se);
                    } finally {
                        timer.tick(1);
                    }
                    if ((i & (s - 1)) == 0) {
                        outputWriter.flush();
                        System.out.printf("[Thread-%d] Completed %d iterations%n", threadId, i + 1);
                    }
                }
                return new RunStats(iterations, timer.getDistribution(), numErrors, threadId);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                outputWriter.flush();
                Closeables.closeQuietly(outputWriter);

            }
        }

        protected abstract void executeIteration(int iteration,PreparedStatement ps) throws SQLException, IOException;

        protected abstract PreparedStatement getPreparedStatement() throws Exception;

        protected void reportSize(int iteration, long size) throws IOException {
            outputWriter.write(String.format("[Iteration-%d] resultSetSize=%d%n",iteration,size));
        }

        protected void reportError(int iteration,SQLException se) throws IOException {
            se.printStackTrace();
            outputWriter.write(String.format("[Iteration-%d] %s%n",iteration, Throwables.getStackTraceAsString(se.getNextException())));
        }
    }

    private static final double NANOS_TO_MILLIS = 1000d*1000d;
    private static final double NANOS_TO_SECONDS = NANOS_TO_MILLIS*1000;
    private static class RunStats{
        private final int threadId;
        private final int numIterations;
        private final DistributionTimeView runTimeData;
        private final int numErrors;

        private RunStats(int numIterations, DistributionTimeView runTimeData, int numErrors,int threadId) {
            this.numIterations = numIterations;
            this.runTimeData = runTimeData;
            this.numErrors = numErrors;
            this.threadId = threadId;
        }

        public void print(){
            System.out.printf("Thread: %d%n",threadId);
            System.out.printf("Total Iterations: %d%n",numIterations);
            System.out.printf("Num Errors: %d%n",numErrors);
            double elapsedSeconds = runTimeData.getWallClockTime() / NANOS_TO_SECONDS;
            System.out.printf("Total Time(s):%f%n", elapsedSeconds);
            System.out.printf("Queries/s:%d%n",(int)(numIterations/elapsedSeconds));
        }
    }
}
