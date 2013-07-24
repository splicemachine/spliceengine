package com.splicemachine.derby.nist;

import java.sql.Connection;

/**
 * @author Jeff Cunningham
 *         Date: 7/23/13
 */
public interface ConnectionFactory {

    Connection getConnection() throws Exception;
}
