package com.splicemachine.management;

import java.sql.SQLException;
import java.util.Properties;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.colperms.ColPermsManager;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;

/**
 * Manager
 */
public interface Manager {

    void enableEnterprise(final char[] value) throws SQLException;

    BackupManager getBackupManager() throws SQLException;

    UserAuthenticator getAuthenticationManager(AuthenticationServiceBase svc, Properties properties) throws SQLException;

    ColPermsManager getColPermsManager() throws SQLException;

}
