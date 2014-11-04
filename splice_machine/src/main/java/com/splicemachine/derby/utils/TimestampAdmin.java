package com.splicemachine.derby.utils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Lists;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.hbase.jmx.JMXUtils;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.execute.IteratorNoPutResultSet;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.si.impl.timestamp.TimestampMasterManagement;
import com.splicemachine.si.impl.timestamp.TimestampRegionManagement;

/**
 * Implementation logic for system procedures associated with our
 * Timestamp Generator for transactions. Most of these procedures
 * are defined in:
 * {@link com.splicemachine.derby.impl.sql.catalog.SpliceSystemProcedures}.
 * 
 * @author Walt Koetke
 */
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
                for (TimestampMasterManagement mgmt : JMXUtils.getTimestampMasterManagement(connections)) {
                	numberOfTimestamps = mgmt.getNumberTimestampsCreated();
                	numberOfBlocks = mgmt.getNumberBlocksReserved();
                }
    			ExecRow row = new ValueRow(2);
    			row.setColumn(1, new SQLLongint(numberOfTimestamps));
    			row.setColumn(2, new SQLLongint(numberOfBlocks));
    			EmbedConnection defaultConn = (EmbedConnection)SpliceAdmin.getDefaultConn();
    			Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
    			IteratorNoPutResultSet rs = new IteratorNoPutResultSet(Arrays.asList(row), TIMESTAMP_GENERATOR_INFO_COLUMNS, lastActivation);
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
                List<Pair<String, TimestampRegionManagement>> mgrs = JMXUtils.getTimestampRegionManagement(connections);
				ExecRow template = new ValueRow(3);
				template.setRowArray(new DataValueDescriptor[]{
					new SQLVarchar(), new SQLDouble(), new SQLDouble()
				});
				List<ExecRow> rows = Lists.newArrayListWithExpectedSize(mgrs.size());
				for (Pair<String, TimestampRegionManagement> mgmtPair : mgrs) {
					TimestampRegionManagement mgmt = mgmtPair.getSecond();
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
