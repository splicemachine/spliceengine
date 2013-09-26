package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.DistinctGroupedAggregateOperationIT;
import org.apache.derby.impl.sql.execute.operations.DistinctScalarAggregateOperationIT;
import org.apache.derby.impl.sql.execute.operations.MultiGroupGroupedAggregateOperationIT;
import org.apache.derby.impl.sql.execute.operations.ScalarAggregateOperationIT;
import org.apache.derby.impl.sql.execute.operations.SingleGroupGroupedAggregateOperationIT;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@RunWith(Categories.class)
@Categories.ExcludeCategory(OperationCategories.Transactional.class)
@Suite.SuiteClasses({
		DistinctGroupedAggregateOperationIT.class,
        DistinctScalarAggregateOperationIT.class,
        SingleGroupGroupedAggregateOperationIT.class,
        MultiGroupGroupedAggregateOperationIT.class,
        ScalarAggregateOperationIT.class
})
public class AggregationSuite { }
