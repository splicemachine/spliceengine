package com.splicemachine.perf.runner.qualifiers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public interface Qualifier {

    void setInto(PreparedStatement ps,int position) throws SQLException;

    void validate(ResultSet rs) throws SQLException;
}
