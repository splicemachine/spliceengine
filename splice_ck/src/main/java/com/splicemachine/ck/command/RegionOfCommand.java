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
import com.splicemachine.derby.utils.EngineUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "regionof",
        descriptionHeading = "Description:%n",
        description = "retrieve HBase region of SpliceMachine table",
        parameterListHeading = "Parameters:%n",
        optionListHeading = "Options:%n")
public class RegionOfCommand extends CommonOptions implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "SpliceMachine schema name")
    String schema;
    @CommandLine.Parameters(index = "1", description = "SpliceMachine table name")
    String table;

    @Override
    public Integer call() throws Exception {
        try {
            HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
            table = EngineUtils.validateTable(table);
            schema = EngineUtils.validateSchema(schema);
            System.out.println(hbaseInspector.regionOf(schema, table));
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, table));
            return -1;
        }
    }
}
