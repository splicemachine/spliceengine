package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.DistinctScanOperationTest;
import org.apache.derby.impl.sql.execute.operations.MetatablesTest;
import org.apache.derby.impl.sql.execute.operations.OperationCategories;
import org.apache.derby.impl.sql.execute.operations.ProjectRestrictOperationTest;
import org.apache.derby.impl.sql.execute.operations.RowCountOperationTest;
import org.apache.derby.impl.sql.execute.operations.SortOperationTest;
import org.apache.derby.impl.sql.execute.operations.TableScanOperationTest;
import org.apache.derby.impl.sql.execute.operations.TimeTableScanTest;
import org.apache.derby.impl.sql.execute.operations.UnionOperationTest;
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
        DistinctScanOperationTest.class,
        ProjectRestrictOperationTest.class,
        TableScanOperationTest.class,
        UnionOperationTest.class,
        SortOperationTest.class,
        MetatablesTest.class,
        TimeTableScanTest.class,
        RowCountOperationTest.class
})
public class ScanSuite { }
