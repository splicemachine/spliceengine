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
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "rget",
        description = "print HBase row history sorted by transaction ID",
        parameterListHeading = "Parameters:%n",
        descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n" )
public class RGetCommand extends CommonOptions implements Callable<Integer>
{
    @CommandLine.Parameters(index = "0", description = "row id") String id;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "row values parsing options%n")
    ExclusiveRowParsing rowParsingGroup;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "table identifier options%n")
    ExclusiveTableName tableNameGroup;

    public static class ExclusiveRowParsing {
        @CommandLine.Option(names = {"-s", "--schema"}, required = true, split =",", description = "user-defined schema, possible values: ${COMPLETION-CANDIDATES}") Utils.SQLType[] schema;
        @CommandLine.Option(names = {"-a", "--auto"}, required = true, description = "retrieve the schema of row automatically") Boolean auto;
        @CommandLine.Option(names = {"-n", "--none"}, required = true, description = "print the row in hex") Boolean none;
    }

    public static class ExclusiveTableName {
        @CommandLine.Option(names = {"-t", "--table"}, required = true, description = "SpliceMachine table name") String table;
        @CommandLine.Option(names = {"-r", "--region"}, required = true, description = "HBase region name (with of without 'splice:' prefix)") String region;
    }

    public RGetCommand() {
    }

    @Override
    public Integer call() throws Exception {
        HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
        try {
            String table;
            String region;
            if(tableNameGroup.table != null) {
                table = tableNameGroup.table;
                region = hbaseInspector.regionOf(table);
            } else {
                if(StringUtils.isNumeric(tableNameGroup.region)) {
                    region = "splice:" + tableNameGroup.region;
                } else {
                    region = tableNameGroup.region;
                }
                table = hbaseInspector.tableOf(region);
            }
            if(rowParsingGroup.auto != null) {
                Utils.Tabular schema = hbaseInspector.schemaOf(table);
                Utils.SQLType[] sqlTypes = Utils.toSQLTypeArray(schema.getCol(2));
                System.out.println(hbaseInspector.scanRow(region, id, sqlTypes));
            } else {
                System.out.println(hbaseInspector.scanRow(region, id, rowParsingGroup.schema /* ok if null */));
            }
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, tableNameGroup.table != null ? tableNameGroup.table : tableNameGroup.region));
            return -1;
        }
    }
}
