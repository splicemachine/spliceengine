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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import splice.com.google.common.collect.Lists;
import com.splicemachine.hbase.jmx.JMXUtils;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;

import java.sql.Types;
import java.util.Collections;
import java.util.List;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.timestamp.api.TimestampClientStatistics;
import com.splicemachine.timestamp.api.TimestampOracleStatistics;
import com.splicemachine.utils.Pair;

/**
 * Implementation logic for system procedures associated with our
 * Timestamp Generator for transactions. Most of these procedures
 * are defined in:
 * {@link com.splicemachine.derby.impl.sql.catalog.SpliceSystemProcedures}.
 * 
 * @author Walt Koetke
 */
@SuppressWarnings("unused")
public class TimestampAdmin extends BaseAdminProcedures {

	private static final ResultColumnDescriptor[] TIMESTAMP_GENERATOR_INFO_COLUMNS = new GenericColumnDescriptor[] {
		new GenericColumnDescriptor("numberTimestampsCreated", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
		new GenericColumnDescriptor("numberBlocksReserved",    DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
	};
	
    public static void SYSCS_GET_TIMESTAMP_GENERATOR_INFO(final ResultSet[] resultSet) throws SQLException {
        operateOnMaster(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                long numberOfTimestamps = -1;
                long numberOfBlocks = -1;
                for (TimestampOracleStatistics mgmt : JMXUtils.getTimestampOracleStatistics(connections)) {
                	numberOfTimestamps = mgmt.getNumberTimestampsCreated();
                	numberOfBlocks = mgmt.getNumberBlocksReserved();
                }
    			ExecRow row = new ValueRow(2);
    			row.setColumn(1, new SQLLongint(numberOfTimestamps));
    			row.setColumn(2, new SQLLongint(numberOfBlocks));
    			EmbedConnection defaultConn = (EmbedConnection)SpliceAdmin.getDefaultConn();
    			Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
    			IteratorNoPutResultSet rs = new IteratorNoPutResultSet(Collections.singletonList(row), TIMESTAMP_GENERATOR_INFO_COLUMNS, lastActivation);
    			try {
    				rs.openCore();
    			} catch (StandardException e) {
    				throw PublicAPI.wrapStandardException(e);
    			}
    			resultSet[0] = new EmbedResultSet40(defaultConn, rs, false, null, true);
            }
        });
    }
    
	private static final ResultColumnDescriptor[] TIMESTAMP_REQUEST_INFO_COLUMNS = new GenericColumnDescriptor[] {
		new GenericColumnDescriptor("hostName",           DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
		new GenericColumnDescriptor("totalRequestCount",  DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
		new GenericColumnDescriptor("avgRequestDuration", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE))
	};
	
	public static void SYSCS_GET_TIMESTAMP_REQUEST_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<Pair<String, TimestampClientStatistics>> mgrs = JMXUtils.getTimestampClientStatistics(connections);
				ExecRow template = new ValueRow(3);
				template.setRowArray(new DataValueDescriptor[]{
					new SQLVarchar(), new SQLDouble(), new SQLDouble()
				});
				List<ExecRow> rows = Lists.newArrayListWithExpectedSize(mgrs.size());
				for (Pair<String, TimestampClientStatistics> mgmtPair : mgrs) {
					TimestampClientStatistics mgmt = mgmtPair.getSecond();
					template.resetRowArray();
					DataValueDescriptor[] dvds = template.getRowArray();
					try {
						dvds[0].setValue(mgmtPair.getFirst()); // region server name
						dvds[1].setValue(mgmt.getNumberTimestampRequests());
						dvds[2].setValue(mgmt.getAvgTimestampRequestDuration());
					} catch (StandardException se) {
						throw PublicAPI.wrapStandardException(se);
					}
					rows.add(template.getClone());
                }

    			EmbedConnection defaultConn = (EmbedConnection)SpliceAdmin.getDefaultConn();
    			Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
    			IteratorNoPutResultSet rs = new IteratorNoPutResultSet(rows, TIMESTAMP_REQUEST_INFO_COLUMNS, lastActivation);
    			try {
    				rs.openCore();
    			} catch (StandardException e) {
    				throw PublicAPI.wrapStandardException(e);
    			}
    			resultSet[0] = new EmbedResultSet40(defaultConn, rs, false, null, true);
            }
        });
    }
}
