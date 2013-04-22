package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class DefaultedSpliceWatcher extends SpliceWatcher{

    private String defaultSchema;

    public DefaultedSpliceWatcher(String schema){
        defaultSchema = schema;
    }

    @Override
    public Connection createConnection() throws Exception {
        Connection conn = super.createConnection();

        PreparedStatement stmt = conn.prepareStatement("SET SCHEMA ?");
        stmt.setString(1, defaultSchema);
        stmt.executeUpdate();

        return conn;
    }
}
