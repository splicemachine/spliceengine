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

package com.splicemachine.ck.command;

import picocli.CommandLine;

/**
 * Common options shared by all commands.
 */
public class CommonOptions {
    @CommandLine.Option(names = {"-z", "--zookeeper-quorum"}, required = true, defaultValue = "localhost", description = "HBase Zookeeper quorum, default value: ${DEFAULT-VALUE}") String zkq;
    @CommandLine.Option(names = {"-p", "--port"}, required = true, defaultValue = "2181",  description = "HBase port, default value: ${DEFAULT-VALUE}") Integer port;
    @CommandLine.Option(names = {"-v", "--verbose"}, required = false, defaultValue = "true",  description = "verbose mode, show more information") public static boolean verbose;
    @CommandLine.Option(names = {"-l", "--colors"}, required = false, defaultValue = "true",  description = "print colored output to terminal") public static boolean colors;
}
