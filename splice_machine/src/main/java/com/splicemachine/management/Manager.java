package com.splicemachine.management;

import java.sql.SQLException;

/**
 * Manager
 */
public interface Manager {

    void enableEnterprise(final char[] value) throws SQLException;
}
