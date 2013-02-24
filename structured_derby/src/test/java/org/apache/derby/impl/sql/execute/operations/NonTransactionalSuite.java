package org.apache.derby.impl.sql.execute.operations;

import org.junit.experimental.categories.Categories;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@Categories.ExcludeCategory(OperationCategories.Transactional.class)
public class NonTransactionalSuite extends AllOperationSuite{ }
