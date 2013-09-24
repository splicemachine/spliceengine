package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.DeleteOperationIT;
import org.apache.derby.impl.sql.execute.operations.InsertOperationIT;
import org.apache.derby.impl.sql.execute.operations.UpdateOperationIT;
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
        InsertOperationIT.class,
        DeleteOperationIT.class,
        UpdateOperationIT.class
})
public class MutationSuite { }
