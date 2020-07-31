package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "rget",
        description = "print hbase row history sorted by time",
        parameterListHeading = "Parameters:%n",
        descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n" )
public class RGetCommand extends ConnectionOptions implements Callable<Integer>
{
    @CommandLine.Parameters(index = "0", description = "row id") String id;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "row values parsing options%n")
    ExclusiveRowParsing rowParsingGroup;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "table identifier options%n")
    ExclusiveTableName tableNameGroup;

    public static class ExclusiveRowParsing {
        @CommandLine.Option(names = "schema", required = true, split =",", description = "user-defined schema, possible values: ${COMPLETION-CANDIDATES}") Utils.SQLType[] schema;
        @CommandLine.Option(names = "auto", required = true, description = "retrieve the schema of row automatically") Boolean inferred;
        @CommandLine.Option(names = "none", required = true, description = "print the row in hex") Boolean none;
    }

    public static class ExclusiveTableName {
        @CommandLine.Option(names = "table", required = true, description = "splice table name") String table;
        @CommandLine.Option(names = "region", required = true, description = "hbase region name (with of without 'splice:' prefix)") String region;
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
                region = hbaseInspector.getHbaseRegionOf(table);
            } else {
                if(StringUtils.isNumeric(tableNameGroup.region)) {
                    region = "splice:" + tableNameGroup.region;
                } else {
                    region = tableNameGroup.region;
                }
                table = hbaseInspector.getSpliceTableNameOf(region);
            }
            if(rowParsingGroup.inferred != null) {
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
