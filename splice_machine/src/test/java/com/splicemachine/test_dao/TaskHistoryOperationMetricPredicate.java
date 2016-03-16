package com.splicemachine.test_dao;

import org.sparkproject.guava.base.Predicate;
import org.junit.Assert;

/**
 * Evaluates to true for TaskHistory having the specified OperationMetric and value.
 */
public class TaskHistoryOperationMetricPredicate implements Predicate<TaskHistory> {

//    private OperationMetric operationMetric;
    private Object value;

//    public TaskHistoryOperationMetricPredicate(OperationMetric operationMetric, Object value) {
//        this.operationMetric = operationMetric;
//        this.value = value;
//    }
//
    @Override
    public boolean apply(TaskHistory taskHistory) {
        Assert.fail("IMPLEMENT");
        return false;
//        Object valueForMetric = taskHistory.getOperationMetricMap().get(operationMetric);
//        return valueForMetric != null && valueForMetric.equals(value);
    }
}
