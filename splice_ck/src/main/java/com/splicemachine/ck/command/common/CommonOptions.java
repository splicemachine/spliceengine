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

package com.splicemachine.ck.command.common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import picocli.CommandLine;

/**
 * Common options shared by all commands.
 */
@SuppressFBWarnings(value = "MS_SHOULD_BE_FINAL", justification = "intentional, static fields are supposed to be read by other components")
public class CommonOptions {
    @CommandLine.Option(names = {"-z", "--zookeeper-quorum"}, required = true, defaultValue = "localhost", description = "HBase Zookeeper quorum, default value is ${DEFAULT-VALUE}") public String zkq;
    @CommandLine.Option(names = {"-p", "--port"}, required = true, defaultValue = "2181",  description = "HBase port, default value is ${DEFAULT-VALUE}") public Integer port;
    @CommandLine.Option(names = {"-v", "--verbose"}, required = false, defaultValue = "false", negatable = true, fallbackValue="true",  description = "verbose mode, show more information, default is ${DEFAULT-VALUE}") public static boolean verbose;
    @CommandLine.Option(names = {"-l", "--color"}, required = false, defaultValue = "true", negatable = true, fallbackValue="true", description = "print colored output to terminal, default is ${DEFAULT-VALUE}") public static boolean colors;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message") public boolean help;
}

