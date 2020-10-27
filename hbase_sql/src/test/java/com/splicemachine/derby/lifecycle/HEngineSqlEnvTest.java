/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.lifecycle;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test static methods in HEngineSqlEnv.
 */

public class HEngineSqlEnvTest {

    String yarnMemorySizeInMB;
    String sparkDynamicAllocationString;
    String executorInstancesString;
    String executorCoresString;
    String sparkExecutorMemory;
    String sparkDynamicAllocationMaxExecutors;
    String sparkExecutorMemoryOverhead;
    String sparkYARNExecutorMemoryOverhead;
    int    numNodes;
    int    actual;

    private int calcExecutorCores() {
        return HEngineSqlEnv.
               calculateMaxExecutorCores(yarnMemorySizeInMB,
                                         sparkDynamicAllocationString,
                                         executorInstancesString,
                                         executorCoresString,
                                         sparkExecutorMemory,
                                         sparkDynamicAllocationMaxExecutors,
                                         sparkExecutorMemoryOverhead,
                                         sparkYARNExecutorMemoryOverhead,
                                         numNodes);
    }

    private void calculate(int expected) {
        actual = calcExecutorCores();
        Assert.assertTrue("Unexpected maxExecutorCores estimate. \n Expected: " + expected + "\nActual: " + actual, actual == expected);
    }

    @Test
    public void testCalculateMaxExecutorCores() {
        numNodes = 1;
        yarnMemorySizeInMB = "11264";
        sparkDynamicAllocationString = "true";
        executorInstancesString = "1";
        executorCoresString = "16";
        sparkExecutorMemory = "10g";
        sparkDynamicAllocationMaxExecutors = "1000";
        sparkExecutorMemoryOverhead = null;
        sparkYARNExecutorMemoryOverhead = null;
        calculate(16);

        yarnMemorySizeInMB = "1024"; calculate(16);
        executorCoresString = ""; calculate(1);
        executorCoresString = null; calculate(1);
        yarnMemorySizeInMB = "3380"; calculate(1);
        sparkExecutorMemory = "1g"; calculate(3);
        sparkDynamicAllocationMaxExecutors = "2"; calculate(2);
        executorCoresString = "2"; calculate(4);
        executorCoresString = "20"; calculate(40);
        executorCoresString = "1";
        yarnMemorySizeInMB = null; sparkDynamicAllocationMaxExecutors = null; calculate(7);
        numNodes = 3; calculate(21);
        sparkExecutorMemoryOverhead = "1000"; calculate(12);
        sparkYARNExecutorMemoryOverhead = "2000"; calculate(6);
        numNodes = 100; calculate(200);
        yarnMemorySizeInMB = "11264"; calculate(300);
        sparkExecutorMemoryOverhead = sparkYARNExecutorMemoryOverhead = null;
        calculate(1000);
        executorCoresString = "10"; calculate(10000);

        numNodes = 1;
        executorCoresString = "1";
        sparkDynamicAllocationString = "false"; calculate(1);
        executorInstancesString = "40";  calculate(10);
        numNodes = 4;  calculate(40);
        numNodes = 1;  yarnMemorySizeInMB = "50000"; calculate(40);
        numNodes = 0; calculate(40);
        numNodes = -1; calculate(40);
        executorCoresString = "5"; calculate(200);
    }

    private void testParse(String sizeString, long defaultValue, String defaultSuffix, long expected) {
        long actual = HEngineSqlEnv.parseSizeString(sizeString, defaultValue, defaultSuffix);
        Assert.assertTrue("Unexpected maxExecutorCores estimate. \n Expected: " + expected + "\nActual: " + actual, actual == expected);
    }

    @Test
    public void testParseSizeString() {
        testParse("17", 1, "b", 17);
        testParse("17b", 1, "b", 17);
        testParse("17B", 1, "b", 17);
        testParse("1k", 1, "b", 1024);
        testParse("1", 1, "b", 1);
        testParse("1kb", 1, "b", 1024);
        testParse("1K", 1, "b", 1024);
        testParse("1KB", 1, "b", 1024);
        testParse("1", 1, "k", 1024);
        testParse("1m", 1, "b", 1048576);
        testParse("1mb", 1, "b", 1048576);
        testParse("1M", 1, "b", 1048576);
        testParse("1MB", 1, "b", 1048576);
        testParse("1", 1, "m", 1048576);
        testParse("1g", 1, "b", 1073741824);
        testParse("1gb", 1, "b", 1073741824);
        testParse("1G", 1, "b", 1073741824);
        testParse("1GB", 1, "b", 1073741824);
        testParse("1", 1, "g", 1073741824);
        testParse("1p", 1, "b", 1125899906842624L);
        testParse("1pb", 1, "b", 1125899906842624L);
        testParse("1P", 1, "b", 1125899906842624L);
        testParse("1PB", 1, "b", 1125899906842624L);
        testParse("1", 1, "p", 1125899906842624L);
        // Max long value.
        testParse("8934", 1, "p", 9223372036854775807L);

        // A zero value is allowed
        testParse("0", 1, "p", 0);
        testParse("0", 1, "g", 0);
        testParse("0", 1, "m", 0);
        testParse("0", 1, "k", 0);
        testParse("0", 1, "b", 0);
        testParse("0B", 1, "b", 0);
        testParse("0K", 1, "b", 0);
        testParse("0M", 1, "b", 0);
        testParse("0T", 1, "b", 0);
        testParse("0P", 1, "b", 0);

        // Illegal input results in default value.
        testParse("h1GB", 1, "m", 1);
        testParse("-1GB", 1, "m", 1);
        testParse("1.5GB", 1, "m", 1);
        testParse("", 1, "m", 1);
        testParse("abc", 123, "g", 123);

        testParse("22G", 1, "b", 23622320128L);
        testParse("1", 123, "g", 1073741824);
        testParse("1", 123, "gb", 1073741824);
        testParse("1", 123, "gb ", 123);

        testParse("1M", 1, "b", 1048576);
        testParse("12345678987654321", 1, "b", 12345678987654321L);
        testParse("65m", 1, "b", 68157440L);
        testParse("65r", 123, "b", 123);
        testParse("1000000000000", 1, "k", 1024000000000000L);
        testParse("1000000000000", 1, "m", 1048576000000000000L);
    }
}


