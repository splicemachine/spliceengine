package com.splicemachine.test.suites;

import org.junit.experimental.categories.Categories;

import com.splicemachine.test.suites.OperationCategories.Transactional;


/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@Categories.ExcludeCategory(OperationCategories.Transactional.class)
public class NonTransactionalSuite extends AllOperationSuite{ }
