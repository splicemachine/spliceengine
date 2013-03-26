package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.OperationCategories;
import org.apache.derby.impl.sql.execute.operations.OperationCategories.Transactional;
import org.apache.derby.impl.sql.execute.operations.joins.*;
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
        BaseJoinTest.class,
        CrossJoinTest.class,
        InnerJoinTest.class,
        MergeSortJoinTest.class,
        NaturalJoinTest.class,
        OuterJoinTest.class,
        SimpleJoinTest.class
})
public class JoinSuite { }
