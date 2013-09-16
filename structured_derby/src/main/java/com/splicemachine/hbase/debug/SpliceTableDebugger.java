package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.AsyncJobScheduler;
import com.splicemachine.encoding.DataType;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobScheduler;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.ValuePredicate;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.BitSet;
import java.util.Collections;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class SpliceTableDebugger extends Configured implements Tool {

    private static final String commandPattern = "%-50s\t%-100s%n";

    @Override
    public int run(String[] args) throws Exception {
        if(args.length<=0)
            return Operation.HELP.execute(getConf(),null,args);
        Operation operation = Operation.getOperation(args[0]);

        if(operation==Operation.HELP)
            return operation.execute(getConf(),null,args);

        SpliceZooKeeperManager zkManager = null;
        JobScheduler<CoprocessorJob> scheduler;
        try{
            zkManager = new SpliceZooKeeperManager();
            scheduler = new AsyncJobScheduler(zkManager,getConf());
            return operation.execute(getConf(),scheduler,args);
        }finally{
            if(zkManager!=null)
                zkManager.close();
        }
    }

    public static void main(String...args) throws Exception{
        System.exit(ToolRunner.run(new Configuration(),new SpliceTableDebugger(),args));
    }

    private enum Operation{
        COUNT("count"){
            @Override
            public CoprocessorJob getJob(Configuration config, String[] args) throws Exception {
                if(args.length!=3){
                    printHelpMessage();
                    return null;
                }

                String tableName = args[1];
                String destinationDirectory = args[2];

                FileSystem fs = FileSystem.get(config);
                Path path = new Path(destinationDirectory);
                if(!fs.exists(path)){
                    System.err.printf("Destination directory %s does not exist%n", destinationDirectory);
                    return null;
                }

                return new NonTransactionalCounterJob(destinationDirectory,tableName);
            }

            @Override
            public void printHelpMessage() {
                System.out.println("Counts the number of rows in the specified table in parallel, ignoring transactions");
                System.out.println("usage: spliceTableDebugger count <tableName> <destinationDirectory>");
                System.out.println("Arguments:");
                System.out.printf(commandPattern,"tableName","The HBase name of the table to count");
                System.out.printf(commandPattern,"destinationDirectory","The Destination directory to dump output to (in hdfs)");
            }
        },
        SCAN("scan"){
            @Override
            public void printHelpMessage() {
                System.out.println("Scans a table for rows which match a given value (in a given column), ignoring transactions");
                System.out.println("usage: spliceTableDebugger scan <tableName> <columnNumber> <columnType> <columnValue> <destinationDirectory>");
                System.out.println("Arguments:");
                System.out.printf(commandPattern,"tableName","The HBase name of the table to count");
                System.out.printf(commandPattern,"destinationDirectory","The Destination directory to dump output to (in hdfs)");
                System.out.printf(commandPattern,"columnNumber", "The column number (indexed from 0) of the column of interest");
                System.out.printf(commandPattern,"columnType", "The Type of the column of interest");
                System.out.printf(commandPattern,"columnValue", "The Unencoded value of the column data");
                DataType.printHelpfulMessage();
            }

            @Override
            public CoprocessorJob getJob(Configuration config, String[] args) throws Exception {
                if(args.length<6){
                    printHelpMessage();
                    return null;
                }

                String tableName = args[1];
                int colNum = Integer.parseInt(args[2]);
                DataType dataType = DataType.fromCode(args[3]);
                String colValue = args[4];
                String destDir = args[5];

                byte[] bytes = dataType.encode(colValue);
                BitSet cols = new BitSet(colNum);
                cols.set(colNum);
                Predicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,colNum,bytes,true);

                EntryPredicateFilter epf = new EntryPredicateFilter(cols, Collections.singletonList(predicate));

                return new ScanJob(destDir,tableName,epf);
            }
        },
        TRANSACTION_COUNT("tcount"),
        HELP("help"){
            @Override
            public int execute(Configuration config,JobScheduler scheduler,String[] args) throws Exception {
                if(args.length!=2){
                    printUsageMessage();
                }else{
                    Operation op = getOperation(args[1]);
                    op.printHelpMessage();
                }
                return 1;
            }

            @Override
            public void printHelpMessage() {
                printUsageMessage();
            }
        };

        public void printHelpMessage() {
            throw new UnsupportedOperationException();
        }

        private final String name;

        private Operation(String name) {
            this.name = name;
        }

        public static Operation getOperation(String typeArg){
            for(Operation op:values()){
                if(op.name.equalsIgnoreCase(typeArg))
                    return op;
            }
            return Operation.HELP;
        }

        public CoprocessorJob getJob(Configuration config, String[] args) throws Exception{
            throw new UnsupportedOperationException();
        }

        public int execute(Configuration config,JobScheduler<CoprocessorJob> scheduler,String[] args) throws Exception{
            CoprocessorJob job = getJob(config,args);
            if(job==null)
                return 2; //failed to properly configure--each job will print its own error messages
            JobFuture submit = scheduler.submit(job);
            int numTasksToFinish = submit.getNumTasks();
            System.out.printf("Executing job with %d tasks%n", numTasksToFinish);
            int remaining;
            do{
                submit.completeNext();
                remaining = submit.getRemainingTasks();
                System.out.printf("%d tasks remaining%n", remaining);
            }while(remaining>0);
            System.out.println("FINISHED");

            return 0;
        }
    }

    private static void printUsageMessage() {
        System.out.printf("usage: spliceTableDebugger <command> [options]%n");
        System.out.printf("Commands:%n");
        System.out.printf(commandPattern,"count","Counts the number of records in a table");
        System.out.printf(commandPattern,"scan","Looks for records which match a given field");
        System.out.printf(commandPattern,"tcount","Generates a count by Transaction id for a given table");
        System.out.printf(commandPattern,"help","Prints this Help Message");
    }
}
