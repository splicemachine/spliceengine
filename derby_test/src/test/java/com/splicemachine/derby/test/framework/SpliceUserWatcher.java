package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.splicemachine.constants.SpliceConstants;

public class SpliceUserWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceUserWatcher.class);
	public String userName;
	public String password;
	public SpliceUserWatcher(String userName, String password) {
		this.userName = userName;
		this.password = password;
	}
	@Override
	protected void starting(Description description) {
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			dropAndCreateUser(userName,password);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(statement);
			DbUtils.commitAndCloseQuietly(connection);
		}
		super.starting(description);
	}
	@Override
	protected void finished(Description description) {
		LOG.trace("Finished");
	}
	
	public void createUser(String userName, String password) {
	  if (!SpliceConstants.authentication.toUpperCase().equals("LDAP")) {
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.prepareStatement("call syscs_util.syscs_create_user(?,?)");
			statement.setString(1, userName);
			statement.setString(2, password);
			statement.execute();
		} catch (Exception e) {
			LOG.error("error Creating " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(statement);
			DbUtils.commitAndCloseQuietly(connection);
		}
	  }
	}
	
	public void dropUser(String userName) {
	  if (SpliceConstants.authentication.toUpperCase().equals("LDAP")) {
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.prepareStatement("select username from sys.sysusers where username = ?");
			statement.setString(1, userName);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				statement = connection.prepareStatement("call syscs_util.syscs_drop_user(?)");
				statement.setString(1, userName);
				statement.execute();
			}
		} catch (Exception e) {
			LOG.error("error Creating " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(statement);
			DbUtils.commitAndCloseQuietly(connection);
		}
	  }
	}
	
	public void dropAndCreateUser(String userName, String password) {
		dropUser(userName);
		createUser(userName,password);
	}
		
}
