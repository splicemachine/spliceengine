package com.splicemachine.test.suites;

import org.apache.derby.impl.sql.execute.operations.DistinctScanOperationIT;
import org.apache.derby.impl.sql.execute.operations.MetatablesIT;
import org.apache.derby.impl.sql.execute.operations.ProjectRestrictOperationIT;
import org.apache.derby.impl.sql.execute.operations.RowCountOperationIT;
import org.apache.derby.impl.sql.execute.operations.SortOperationIT;
import org.apache.derby.impl.sql.execute.operations.TableScanOperationIT;
import org.apache.derby.impl.sql.execute.operations.TimeTableScanIT;
import org.apache.derby.impl.sql.execute.operations.UnionOperationIT;
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
	DistinctScanOperationIT.class,
        ProjectRestrictOperationIT.class,
        TableScanOperationIT.class,
        UnionOperationIT.class,
        SortOperationIT.class,
        MetatablesIT.class,
        TimeTableScanIT.class,
        RowCountOperationIT.class
})
public class ScanSuite { }
