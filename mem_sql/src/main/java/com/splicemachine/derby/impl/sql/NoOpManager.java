package com.splicemachine.derby.impl.sql;

import java.sql.SQLException;
import java.util.Properties;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
import com.splicemachine.management.Manager;

/**
 * Fake manager impl for mem db
 */
public class NoOpManager implements Manager {
    private static NoOpManager ourInstance=new NoOpManager();

    public static NoOpManager getInstance(){
        return ourInstance;
    }

    private NoOpManager(){ }

    @Override
    public void enableEnterprise(char[] value) throws SQLException {
        // no-op
    }

    @Override
    public BackupManager getBackupManager() throws SQLException {
        return NoOpBackupManager.getInstance();
    }

    @Override
    public UserAuthenticator getAuthenticationManager(AuthenticationServiceBase svc, Properties properties) throws SQLException {
        return null;
    }
}
