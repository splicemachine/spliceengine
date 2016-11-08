/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.derby.test.framework.*;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Created by jleach on 6/1/16.
 */
public class LimitOffsetVisitorIT extends SpliceUnitTest {
    public static final String CLASS_NAME = LimitOffsetVisitorIT.class.getSimpleName().toUpperCase();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_1 = "A";
    public static final String TABLE_2 = "B";
    public static final String TABLE_2_IX = "BIX";
    public static final String TABLE_3 = "C";
    public static final String TABLE_4 = "D";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1,CLASS_NAME, "(col1 int, col2 int, col3 int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2,CLASS_NAME, "(col1 int, col2 int, col3 int)");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_3,CLASS_NAME, "(col1 int, col2 int, col3 int, primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_4,CLASS_NAME, "(col1 int, col2 int, col3 int, primary key (col1))");
    protected static SpliceIndexWatcher spliceIndexWatcher2 = new SpliceIndexWatcher(TABLE_2,
            spliceSchemaWatcher.schemaName,TABLE_2_IX,spliceSchemaWatcher.schemaName,"(col1)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceIndexWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(spliceSchemaWatcher.schemaName);

    @Test
    public void limitOverSimpleSelect() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4},"explain select top 10 * from A",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,");
    }

    @Test
    public void limitOverSimpleProjection() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5},"explain select top 4 * from A where col1+1 < 2",methodWatcher,
                "rows=4,","outputRows=4,","outputRows=4,","outputRows=4,","outputRows=11,");
    }

    @Test
    public void limitOverIndexLookup() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5},"explain select top 10 * from B --splice-properties index=BIX\n",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,");
    }

    @Test
    public void limitOverDistinctScan() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4},"explain select top 10 col1 from A group by col1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=20,");
    }

    @Test
    public void limitOverGroupBy() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6,7},"explain select top 10 col1,max(col2) from A group by col1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=20,","outputRows=20,",
                "outputRows=20,","outputRows=20,");
    }

    @Test
    public void limitOverOrderBy() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5},"explain select top 10 col1 from A order by col1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=20,");
    }

    @Test
    @Ignore("DB-5169")
    public void limitOverMergeSortJoin() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6},"explain select top 10 * from A,B --splice-properties joinStrategy=SORTMERGE\n" +
                        " where A.COL1 = B.COL1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=20,","outputRows=20,");
    }

    @Test
    @Ignore("DB-5169")
    public void limitOverMergeJoin() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6},"explain select top 10 * from C,D --splice-properties joinStrategy=MERGE\n" +
                        " where C.COL1 = D.COL1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,");
    }

    @Test
    @Ignore("DB-5169")
    public void limitOverNLJJoin() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6},"explain select top 10 * from --splice-properties joinOrder=fixed\n C,D --splice-properties joinStrategy=NESTEDLOOP\n" +
                        " where C.COL1 = D.COL1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,");
    }

    @Test
    @Ignore("DB-5169")
    public void limitOverBroadcastJoin() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6},"explain select top 10 * from --splice-properties joinOrder=fixed\n C,D --splice-properties joinStrategy=BROADCAST\n" +
                        " where C.COL1 = D.COL1",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,");
    }

    @Test
    @Ignore("DB-5169")
    public void limitOverUnionAll() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6},"explain select top 10 * from A UNION ALL select * from B",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=10,");
    }

    @Test
    @Ignore("DB-5169")
    public void limitOverUnion() throws Exception {
        rowContainsQuery(new int[]{1,2,3,4,5,6,7},"explain select top 10 * from A UNION select * from B",methodWatcher,
                "rows=10,","outputRows=10,","outputRows=10,","outputRows=10,","outputRows=40,","outputRows=20,",
                "outputRows=20,");
    }

}
