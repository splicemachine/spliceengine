package com.splicemachine.derby.authentication;

import java.sql.SQLException;
import java.util.Properties;
import com.splicemachine.db.authentication.UserAuthenticator;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceUserAuthentication implements UserAuthenticator {
	private static Logger LOG = Logger.getLogger(SpliceUserAuthentication.class);
	@Override
	public boolean authenticateUser(String userName, String userPassword,String databaseName, Properties info) throws SQLException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "authenticate user %s",userName);
		return true; // Let everyone in for now...
	}

}
