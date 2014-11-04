package com.splicemachine.test_dao;

import com.google.common.base.Predicate;
import com.splicemachine.derby.metrics.OperationMetric;

/**
 * Evaluates to true for TaskHistory having the specified OperationMetric and value.
 */
public class TaskHistoryOperationMetricPredicate implements Predicate<TaskHistory> {

    private OperationMetric operationMetric;
    private Object value;

    public TaskHistoryOperationMetricPredicate(OperationMetric operationMetric, Object value) {
        this.operationMetric = operationMetric;
        this.value = value;
    }

    @Override
    public boolean apply(TaskHistory taskHistory) {
        Object valueForMetric = taskHistory.getOperationMetricMap().get(operationMetric);
        return valueForMetric != null && valueForMetric.equals(value);
    }
}
