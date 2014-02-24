package com.splicemachine.si;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 *         Date: 2/20/14
 */
@RunWith(Categories.class)
@Categories.IncludeCategory(ActiveTransactions.class)
@Suite.SuiteClasses({SITransactorTest.class})
public class MultiTestTransactorSuite { }
