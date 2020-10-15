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

package com.splicemachine.derby.utils;

import splice.com.google.common.collect.Lists;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.utils.Pair;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 11/14/14
 */
public class PipelineAdmin extends BaseAdminProcedures{
    private static final ResultColumnDescriptor[] WRITE_INTAKE_COLUMNS = {
            new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("depThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("indThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("depCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("indCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),            
            new GenericColumnDescriptor("totalRejected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };
    public static void SYSCS_GET_WRITE_INTAKE_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new BaseAdminProcedures.JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<PipelineDriver.ActiveWriteHandlersIface> writeHandlers = JMXUtils.getActiveWriteHandlers(connections);
                ExecRow template = buildExecRow(WRITE_INTAKE_COLUMNS);
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(writeHandlers.size());
                int i=0;
                for (PipelineDriver.ActiveWriteHandlersIface writeHandler : writeHandlers) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try{
                        dvds[0].setValue(connections.get(i).getFirst());     
                        dvds[1].setValue(writeHandler.getDependentWriteThreads());
                        dvds[2].setValue(writeHandler.getIndependentWriteThreads());
                        dvds[3].setValue(writeHandler.getDependentWriteCount());
                        dvds[4].setValue(writeHandler.getIndependentWriteCount());
                        dvds[5].setValue(writeHandler.getTotalRejected());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    }
                    rows.add(template.getClone());
                    i++;
                }

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, WRITE_INTAKE_COLUMNS,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);
                resultSet[0] = ers;
            }
        });
    }

}
