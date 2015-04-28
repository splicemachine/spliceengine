package com.splicemachine.derby.utils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.jmx.JMXUtils;

/**
 * Implementation of wrapper class for JMX management statistics of Database Properties.
 *
 * @author David Winters
 * Date: 4/23/2015
 */
public class DatabasePropertyManagementImpl implements DatabasePropertyManagement {

	// Registered instance of the JMX MBean.
	private static DatabasePropertyManagementImpl mBean = new DatabasePropertyManagementImpl();

	/**
	 * Register this implementation under JMX.
	 *
	 * @param mbs the MBeanServer to use
	 * @throws MalformedObjectNameException
	 * @throws NotCompliantMBeanException
	 * @throws InstanceAlreadyExistsException
	 * @throws MBeanRegistrationException
	 */
	public static void registerJMX(MBeanServer mbs)
			throws MalformedObjectNameException,
			NotCompliantMBeanException,
			InstanceAlreadyExistsException,
			MBeanRegistrationException {
		mbs.registerMBean(mBean, new ObjectName(JMXUtils.DATABASE_PROPERTY_MANAGEMENT));
	}

	@Override
	public String getDatabaseProperty(String key) throws SQLException {
		Connection dbConn = SpliceDriver.driver().getInternalConnection();
		CallableStatement stmt = dbConn.prepareCall("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(?)");
		ResultSet rs = null;
		String value = null;
		try {
			stmt.setString(1, key);
			rs = stmt.executeQuery();
			while (rs.next()) {
				value = rs.getString(1);
			}
		} finally {
			rs.close();
			stmt.close();
		}
		return value;
	}

	@Override
	public void setDatabaseProperty(String key, String value) throws SQLException {
		Connection dbConn = SpliceDriver.driver().getInternalConnection();
		CallableStatement stmt = dbConn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
		try {
			stmt.setString(1, key);
			stmt.setString(2, value);
			stmt.executeUpdate();
		} finally {
			stmt.close();
		}
		dbConn.commit();
	}
}
