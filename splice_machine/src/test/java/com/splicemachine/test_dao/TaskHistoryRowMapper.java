package com.splicemachine.test_dao;

import com.splicemachine.derby.metrics.OperationMetric;

import java.sql.ResultSet;
import java.sql.SQLException;

class TaskHistoryRowMapper implements RowMapper<TaskHistory> {

    @Override
    public TaskHistory map(ResultSet resultSet) throws SQLException {
        TaskHistory taskHistory = new TaskHistory();

        int c = 1;
        taskHistory.setStatementId(resultSet.getLong(c++));
        taskHistory.setOperationId(resultSet.getLong(c++));
        taskHistory.setTaskId(resultSet.getLong(c++));
        taskHistory.setHost(resultSet.getString(c++));
        taskHistory.setRegion(resultSet.getString(c));

        for (OperationMetric metric : OperationMetric.values()) {
            taskHistory.getOperationMetricMap().put(metric, resultSet.getObject(metric.getPosition() + 6));
        }

        return taskHistory;
    }

}
