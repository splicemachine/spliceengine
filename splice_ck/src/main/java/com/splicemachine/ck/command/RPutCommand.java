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
import com.splicemachine.ck.encoder.RPutConfigBuilder;
import com.splicemachine.derby.utils.EngineUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;


@CommandLine.Command(name = "rput",
        description = "modify HBase row, (warning: this command can potentially break your database in various different ways, use it on your own risk)",
        parameterListHeading = "Parameters:%n",
        descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n",
        sortOptions = true )
public class RPutCommand extends CommonOptions implements Callable<Integer>
{
    @CommandLine.Parameters(index = "0", description = "row id") String id;
    @CommandLine.Parameters(index = "1", description = "id of commit transaction") Long txn;
    @CommandLine.ArgGroup(validate = false, multiplicity = "1", heading = "row input values options:%n")
    RowOptions rowOptions;
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1", heading = "table identifier options%n")
    TableNameGroup tableNameGroup;

    public static class RowOptions {
        @CommandLine.Option(names = {"-b", "--tombstone"}, required = false, description = "set tombstone flag") Boolean tombstone;
        @CommandLine.Option(names = {"-i", "--anti-tombstone"}, required = false, description = "set anti-tombstone flag") Boolean antiTombstone;
        @CommandLine.Option(names = {"-f", "--first-write"}, required = false, description = "set first-write flag") Boolean firstWrite;
        @CommandLine.Option(names = {"-d", "--delete-after-first-write"}, required = false, description = "set delete-after-first-write flag") Boolean deleteAfterFirstWrite;
        @CommandLine.Option(names = {"-k", "--fk-counter"}, required = false, description = "set foreign-key counter") Long fKCounter;
        @CommandLine.Option(names = {"-u", "--user-data"}, required = false, description = "set user data, example: ('string', 42, false, '2020-10-10 01:02:03.3333')") String userData;
        @CommandLine.Option(names = {"-c", "--commit-timestamp"}, required = false, description = "commit timestamp of the txn") Long commitTS;
    }

    public RPutCommand() {
    }

    @Override
    public Integer call() throws Exception {
        HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
        try {
            String region;
            if(tableNameGroup.qualifiedTableName != null) {
                tableNameGroup.qualifiedTableName.table = EngineUtils.validateTable(tableNameGroup.qualifiedTableName.table);
                tableNameGroup.qualifiedTableName.schema = EngineUtils.validateSchema(tableNameGroup.qualifiedTableName.schema);
                String table = tableNameGroup.qualifiedTableName.table;
                region = hbaseInspector.regionOf(tableNameGroup.qualifiedTableName.schema, table);
            } else {
                if(StringUtils.isNumeric(tableNameGroup.region)) {
                    region = "splice:" + tableNameGroup.region;
                } else {
                    region = tableNameGroup.region;
                }
            }
            RPutConfigBuilder configBuilder = new RPutConfigBuilder();
            configBuilder.withCommitTS(txn);
            if(rowOptions.tombstone != null) {
                configBuilder.withTombstone();
                Utils.tell("tombstone option is set");
            }
            if(rowOptions.antiTombstone != null) {
                configBuilder.withAntiTombstone();
                Utils.tell("anti-tombstone option is set");
            }
            if(rowOptions.firstWrite != null) {
                configBuilder.withFirstWrite();
                Utils.tell("first write option is set");
            }
            if(rowOptions.fKCounter != null) {
                configBuilder.withForeignKeyCounter(rowOptions.fKCounter);
                Utils.tell("foreign key counter option is set");            }
            if(rowOptions.deleteAfterFirstWrite != null) {
                configBuilder.withDeleteAfterFirstWrite();
                Utils.tell("delete after first write option is set");
            }
            if(rowOptions.userData != null) {
                configBuilder.withUserData(rowOptions.userData);
                Utils.tell("user data option is set to: ", rowOptions.userData);
            }
            if(rowOptions.commitTS != null) {
                configBuilder.withCommitTS(rowOptions.commitTS);
                Utils.tell("commit timestamp option is set to: ", rowOptions.commitTS.toString());
            }
            hbaseInspector.putRow(region, id, txn, configBuilder.getConfig());
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, tableNameGroup.qualifiedTableName != null ? tableNameGroup.qualifiedTableName.table : tableNameGroup.region));
            return -1;
        }
    }
}
