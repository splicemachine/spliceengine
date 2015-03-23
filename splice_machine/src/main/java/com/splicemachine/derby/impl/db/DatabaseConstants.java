package com.splicemachine.derby.impl.db;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.conf.Configuration;

/**
 * Convenience class for holding constants related to the Derby database stuff
 *
 * @author Scott Fines
 *         Date: 3/17/15
 */
public class DatabaseConstants extends SpliceConstants{
    static{
        setParameters(config);
    }

    //debug options
    /**
     * For debugging an operation, this will force the query parser to dump any generated
     * class files to the HBASE_HOME directory. This is not useful for anything except
     * debugging certain kinds of errors, and is NOT recommended enabled in a production
     * environment.
     *
     * Defaults to false (off)
     */
    @Parameter private static final String DEBUG_DUMP_CLASS_FILE = "splice.debug.dumpClassFile";
    @DefaultValue(DEBUG_DUMP_CLASS_FILE) public static final boolean DEFAULT_DUMP_CLASS_FILE=false;
    public static boolean dumpClassFile;

    @Parameter private static final String DEBUG_DUMP_BIND_TREE = "splice.debug.compile.dumpBindTree";
    @DefaultValue(DEBUG_DUMP_BIND_TREE) public static final boolean DEFAULT_DUMP_BIND_TREE=false;
    public static boolean dumpBindTree;

    @Parameter private static final String DEBUG_DUMP_OPTIMIZED_TREE = "splice.debug.compile.dumpOptimizedTree";
    @DefaultValue(DEBUG_DUMP_OPTIMIZED_TREE) public static final boolean DEFAULT_DUMP_OPTIMIZED_TREE=false;
    public static boolean dumpOptimizedTree;
    /**
     * For debugging statements issued in derby.  This is on by default, but will hurt you in the case of an OLTP
     * workload.
     */
    @Parameter private static final String DEBUG_LOG_STATEMENT_CONTEXT = "splice.debug.logStatementContext";
    @DefaultValue(DEBUG_LOG_STATEMENT_CONTEXT) public static final boolean DEFAULT_LOG_STATEMENT_CONTEXT=true;
    public static boolean logStatementContext;

    public static void setParameters(Configuration config){
        dumpClassFile = config.getBoolean(DEBUG_DUMP_CLASS_FILE,DEFAULT_DUMP_CLASS_FILE);
        dumpBindTree = config.getBoolean(DEBUG_DUMP_BIND_TREE,DEFAULT_DUMP_BIND_TREE);
        dumpOptimizedTree = config.getBoolean(DEBUG_DUMP_OPTIMIZED_TREE,DEFAULT_DUMP_OPTIMIZED_TREE);
        logStatementContext = config.getBoolean(DEBUG_LOG_STATEMENT_CONTEXT,DEFAULT_LOG_STATEMENT_CONTEXT);
    }
}
