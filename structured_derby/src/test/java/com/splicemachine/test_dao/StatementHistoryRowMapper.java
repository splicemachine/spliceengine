package com.splicemachine.test_dao;

import java.sql.ResultSet;
import java.sql.SQLException;

class StatementHistoryRowMapper implements RowMapper<StatementHistory> {

    @Override
    public StatementHistory map(ResultSet resultSet) throws SQLException {
        StatementHistory statementHistory = new StatementHistory();

        int c = 1;
        statementHistory.setStatementId(resultSet.getLong(c++));
        statementHistory.setHost(resultSet.getString(c++));
        statementHistory.setUsername(resultSet.getString(c++));
        statementHistory.setTransactionId(resultSet.getLong(c++));
        statementHistory.setStatus(resultSet.getString(c++));
        statementHistory.setStatementSql(resultSet.getString(c++));
        statementHistory.setTotalJobCount(resultSet.getInt(c++));
        statementHistory.setSuccessfulJobs(resultSet.getInt(c++));
        statementHistory.setFailedJobs(resultSet.getInt(c++));
        statementHistory.setCanceledJobs(resultSet.getInt(c++));
        statementHistory.setStartTimeMs(resultSet.getLong(c++));
        statementHistory.setStopTimeMs(resultSet.getLong(c));

        return statementHistory;
    }


}
