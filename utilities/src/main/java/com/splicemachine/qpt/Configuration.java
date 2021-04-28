package com.splicemachine.qpt;

public class Configuration {
    static final int DEFAULT_ITERATIONS = 1;
    static final int DEFAULT_PARALLELISM = 1;
    static final String DEFAULT_URL = "jdbc:splice://localhost:1527/splicedb";
    static final String DEFAULT_USER = "splice";
    static final String DEFAULT_PASSWORD = "admin";

    enum ExplainMode {NONE, ONCE, ALL};

    enum PrepMode {
        NONE,    // don't prepare
        WHOLE,   // prepare the entire statement as is
        AUTO,    // prepare with auto-parameterization
        SUBST;   // prepare with parameter substitution
    };

    static String URL = DEFAULT_URL;
    static String user = DEFAULT_USER;
    static String password = DEFAULT_PASSWORD;
    static boolean useSSL = false;
    static Boolean useSpark = null;
    static String schema = null;
    static int iterations = DEFAULT_ITERATIONS;
    static boolean execute = true;
    static boolean autocommit = false;
    static boolean batching = false;
    static ExplainMode explain = ExplainMode.NONE;
    static boolean quiet = false;
    static boolean verbose = false;
    static int warmup = 0;
    static int parallelism = DEFAULT_PARALLELISM;
    static boolean collectLatencies = false;
    static PrepMode prepare = PrepMode.NONE;
    static int timeout = 0;
}
