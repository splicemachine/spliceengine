package com.splicemachine.derby.authentication;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.derby.authentication.UserAuthenticator;

public class SpliceUserAuthentication implements UserAuthenticator {
	@Override
	public boolean authenticateUser(String userName, String userPassword,String databaseName, Properties info) throws SQLException {
		return true; // Let everyone in for now...
	}

}
