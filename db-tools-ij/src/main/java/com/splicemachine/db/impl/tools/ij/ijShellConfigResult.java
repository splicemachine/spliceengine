package com.splicemachine.db.impl.tools.ij;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * Marker class to notify for shell-specific command execution.
 */
public class ijShellConfigResult extends ijResultImpl {

    @Override
    public SQLWarning getSQLWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearSQLWarnings() throws SQLException {
    }
}
