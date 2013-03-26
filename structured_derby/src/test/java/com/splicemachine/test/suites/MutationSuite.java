package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.DeleteOperationTest;
import org.apache.derby.impl.sql.execute.operations.InsertOperationTest;
import org.apache.derby.impl.sql.execute.operations.OperationCategories;
import org.apache.derby.impl.sql.execute.operations.UpdateOperationTest;
import org.apache.derby.impl.sql.execute.operations.OperationCategories.Transactional;
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
        InsertOperationTest.class,
        DeleteOperationTest.class,
        UpdateOperationTest.class
})
public class MutationSuite { }
