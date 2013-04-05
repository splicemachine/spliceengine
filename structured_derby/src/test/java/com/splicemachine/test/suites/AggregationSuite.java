package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.DistinctGroupedAggregateOperationTest;
import org.apache.derby.impl.sql.execute.operations.DistinctScalarAggregateOperationTest;
import org.apache.derby.impl.sql.execute.operations.MultiGroupGroupedAggregateOperationTest;
import org.apache.derby.impl.sql.execute.operations.ScalarAggregateOperationTest;
import org.apache.derby.impl.sql.execute.operations.SingleGroupGroupedAggregateOperationTest;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.splicemachine.test.suites.OperationCategories.Transactional;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@RunWith(Categories.class)
@Categories.ExcludeCategory(OperationCategories.Transactional.class)
@Suite.SuiteClasses({
        DistinctGroupedAggregateOperationTest.class,
        DistinctScalarAggregateOperationTest.class,
        SingleGroupGroupedAggregateOperationTest.class,
        MultiGroupGroupedAggregateOperationTest.class,
        ScalarAggregateOperationTest.class
})
public class AggregationSuite { }
