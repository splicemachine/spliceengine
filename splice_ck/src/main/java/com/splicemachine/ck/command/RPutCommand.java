package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import com.splicemachine.ck.encoder.RPutConfigBuilder;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "rput",
        description = "modify hbase row",
        parameterListHeading = "Parameters:%n",
        descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n",
        sortOptions = true )
public class RPutCommand extends ConnectionOptions implements Callable<Integer>
{
    @CommandLine.Parameters(index = "0", description = "row id") String id;
    @CommandLine.Parameters(index = "1", description = "id of commit transaction") Long txn;
    @CommandLine.ArgGroup(validate = false, multiplicity = "1", heading = "row input values options:%n")
    RowOptions rowOptions;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "table identifier options:%n")
    ExclusiveTableName tableNameGroup;

    public static class RowOptions {
        @CommandLine.Option(names = {"tombstone", "tos"}, required = false, description = "set tombstone flag") Boolean tombstone;
        @CommandLine.Option(names = {"anti-tombstone", "at"}, required = false, description = "set anti-tombstone flag") Boolean antiTombstone;
        @CommandLine.Option(names = {"first-write", "fw"}, required = false, description = "set first-write flag") Boolean firstWrite;
        @CommandLine.Option(names = {"delete-after-first-write", "da"}, required = false, description = "set delete-after-first-write flag") Boolean deleteAfterFirstWrite;
        @CommandLine.Option(names = {"fk-counter", "fk"}, required = false, description = "set foreign-key counter") Long fKCounter;
        @CommandLine.Option(names = {"user-data", "ud"}, required = false, description = "set user data, example: ('string', 42, false, Timestamp('2020-10-10 01:02:03.3333'))") String userData;
        @CommandLine.Option(names = {"commit-timestamp", "cts"}, required = false, description = "commit timestamp of the txn") Long commitTS;
    }

    public static class ExclusiveTableName {
        @CommandLine.Option(names = "table", required = true, description = "splice table name") String table;
        @CommandLine.Option(names = "region", required = true, description = "hbase region name (with of without 'splice:' prefix)") String region;
    }

    public RPutCommand() {
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
            RPutConfigBuilder configBuilder = new RPutConfigBuilder();
            configBuilder.withCommitTS(txn);
            if(rowOptions.tombstone != null) {
                configBuilder.withTombstone();
                System.out.println("tombstone is set");
            }
            if(rowOptions.antiTombstone != null) {
                configBuilder.withAntiTombstone();
                System.out.println("anti tombstone is set");
            }
            if(rowOptions.firstWrite != null) {
                configBuilder.withFirstWrite();
                System.out.println("firstWrite is set");
            }
            if(rowOptions.fKCounter != null) {
                configBuilder.withForeignKeyCounter(rowOptions.fKCounter);
                System.out.println("foreignKeyCounter is set");
            }
            if(rowOptions.deleteAfterFirstWrite != null) {
                configBuilder.withDeleteAfterFirstWrite();
                System.out.println("deleteAfterFirstWrite is set");
            }
            if(rowOptions.userData != null) {
                configBuilder.withUserData(rowOptions.userData);
                System.out.println("userData is set");
            }
            if(rowOptions.commitTS != null) {
                configBuilder.withCommitTS(rowOptions.commitTS);
                System.out.println("commitTS is set");
            }
            hbaseInspector.putRow(table, region, id, txn, configBuilder.getConfig());
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, tableNameGroup.table != null ? tableNameGroup.table : tableNameGroup.region));
            return -1;
        }
    }
}
