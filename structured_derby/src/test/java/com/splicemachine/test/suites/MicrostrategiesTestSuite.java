package com.splicemachine.test.suites;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.splicemachine.derby.impl.sql.execute.operations.microstrategy.MicostrategiesCustomerIT;
import com.splicemachine.derby.impl.sql.execute.operations.microstrategy.MicrostrategiesItemIT;
import com.splicemachine.derby.impl.sql.execute.operations.microstrategy.MsOrderDetailIT;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@RunWith(Categories.class)
@Suite.SuiteClasses({MicrostrategiesItemIT.class,MicostrategiesCustomerIT.class,MsOrderDetailIT.class})
public class MicrostrategiesTestSuite { }
