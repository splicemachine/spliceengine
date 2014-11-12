package com.splicemachine.perf.runner.qualifiers;

import java.io.PrintStream;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
public interface Result {

    public void write(PrintStream stream) throws Exception;
}
