package com.splicemachine.test_tools;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 8/28/15
 */
public interface RowCreator{

    /**
     * @return {@code false} if there are no more rows to work with
     */
    boolean advanceRow();

    /**
     * @return the number of rows to work with at one time
     */
    int batchSize();

    /**
     * Set the value for this row into the prepared statement.
     *
     * DO NOT CLOSE the statement! It is managed elsewhere.
     *
     * @param ps the statement to work with
     * @throws SQLException if something goes wrong
     */
    void setRow(PreparedStatement ps) throws SQLException;

    void reset();
}
