package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.OperationCategories;
import org.apache.derby.impl.sql.execute.operations.OperationCategories.Transactional;
import org.junit.experimental.categories.Categories;


/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@Categories.ExcludeCategory(OperationCategories.Transactional.class)
public class NonTransactionalSuite extends AllOperationSuite{ }
