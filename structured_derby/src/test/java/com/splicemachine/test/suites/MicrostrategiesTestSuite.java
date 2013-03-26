package com.splicemachine.test.suites;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.splicemachine.derby.impl.sql.execute.operations.microstrategy.MicostrategiesCustomerTest;
import com.splicemachine.derby.impl.sql.execute.operations.microstrategy.MicrostrategiesItemTest;
import com.splicemachine.derby.impl.sql.execute.operations.microstrategy.MsOrderDetailTest;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
@RunWith(Categories.class)
@Suite.SuiteClasses({MicrostrategiesItemTest.class,MicostrategiesCustomerTest.class,MsOrderDetailTest.class})
public class MicrostrategiesTestSuite { }
