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

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import com.splicemachine.ck.command.common.CommonOptions;
import com.splicemachine.ck.command.common.TableNameGroup;
import org.apache.hadoop.hbase.TableNotFoundException;
import picocli.CommandLine;

import java.io.UncheckedIOException;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "rget",
        description = "print HBase row history sorted by transaction ID",
        parameterListHeading = "Parameters:%n",
        descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n" )
public class RGetCommand extends CommonOptions implements Callable<Integer>
{
    @CommandLine.Parameters(index = "0", description = "row id or 'all' to display all rows (limited by -L)") String id;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "row values parsing options%n")
    ExclusiveRowParsing rowParsingGroup;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "table identifier options%n")
    TableNameGroup tableNameGroup;

    @CommandLine.Option(names = {"-L", "--limit"}, required = false, description =
            "maximum number of rows to print (default is 100)", defaultValue = "100") Long limit;

    @CommandLine.Option(names = {"-V", "--versions"}, required = false, description =
            "versions to display (default 0 = all)", defaultValue = "0") Long versions;

    @CommandLine.Option(names = {"--hbase"}, required = false,
            description = "output data like hbase") Boolean hbaseOption;

    public static class ExclusiveRowParsing {
        @CommandLine.Option(names = {"-c", "--columns"}, required = true, split =",",
                description = "user-defined table columns, possible values: ${COMPLETION-CANDIDATES}")
            Utils.SQLType[] colsSchema;
        @CommandLine.Option(names = {"-a", "--auto"}, required = true,
                description = "retrieve table columns automatically") Boolean auto;
        @CommandLine.Option(names = {"-n", "--none"}, required = true,
                description = "print the row in hex") Boolean none;
    }

    public RGetCommand() {
    }

    @Override
    public Integer call() throws Exception {
        HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
        try {
            String region = tableNameGroup.getRegion(hbaseInspector);
            boolean hbase = hbaseOption != null;

            String rowKey = id.equals("all") ? null : id;
            if(rowParsingGroup.auto != null) {
                Utils.Tabular cols = null;
                try {
                    cols = hbaseInspector.columnsOf(region);
                }
                catch(Exception e)
                {
                    if (e instanceof TableNotFoundException ||
                            (e instanceof UncheckedIOException && e.getCause() instanceof TableNotFoundException)) {
                        rowParsingGroup.auto = null;
                        cols = null;
                        System.out.println("WARNING: couldn't find schema of " + region + ", will print rows as hex");
                    }
                }
                if( cols != null ) {
                    Utils.SQLType[] sqlTypes = Utils.toSQLTypeArray(cols.getCol(2));
                    System.out.println(hbaseInspector.scanRow(region, rowKey, sqlTypes,
                            limit.intValue(), versions.intValue(), hbase ));
                }
            }

            if(rowParsingGroup.auto == null) {
                System.out.println(hbaseInspector.scanRow(region, rowKey, rowParsingGroup.colsSchema /* ok if null */,
                        limit.intValue(), versions.intValue(), hbase));
            }
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, tableNameGroup.qualifiedTableName != null
                    ? tableNameGroup.qualifiedTableName.table
                    : tableNameGroup.region));
            return -1;
        }
    }
}
