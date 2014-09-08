package com.splicemachine.test_dao;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowMapper<T> {

    public T map(ResultSet resultSet) throws SQLException;

}
