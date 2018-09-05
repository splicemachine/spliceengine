package com.splicemachine.db.impl.sql.compile;


import com.splicemachine.db.impl.sql.misc.CommentStripper;
import com.splicemachine.db.impl.sql.misc.CommentStripperImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by yxia on 8/30/18.
 */
public class CommentStripTest {
    private CommentStripper commentStripper = new CommentStripperImpl();

    @Test
    public void testCommentToBeStripped() throws Exception {
        String sql = "select /* test1 */ * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select  * from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        sql = "select /* test1 */ * from t1\n where /* here are the where conditions */ a1=1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = "select  * from t1\n where  a1=1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testNestedComments() throws Exception {
        String sql = "select /*/* test2*/ */ * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select  * from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }


    @Test
    public void testQuotationInComment() throws Exception {
        String sql = "select /* 'xxx'*/ * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select  * from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testDashDashInComment() throws Exception {
        String sql = "select /* -- everything enclosed in the slash-star pair should be stripped off  */ * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select  * from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testSingleQuotationMarkInComment() throws Exception {
        String sql = "select /* everything including single quotation mark ' in comment should be stripped off  */ * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select  * from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    /* negative test case */
    @Test
    public void testQueryWithoutClosingCommentMark() throws Exception {
        String sql = "select /* test1 * from t1;";
        try {
            String sqlTrimmed = commentStripper.stripStatement(sql);
            Assert.fail(String.format("String: %s should not be parsed successfully!", sql));
        } catch (Throwable e) {
            Assert.assertTrue(String.format("Actual error hit: %s, expected: Lexical error ...", e.getMessage()),
                    (e instanceof com.splicemachine.db.impl.sql.misc.TokenMgrError) && (e.getMessage().startsWith("Lexical error")));
        }

        /* nested comments without matching pairs */
        sql = "select /* test1 /* nested comment */ from t1;";
        try {
            String sqlTrimmed = commentStripper.stripStatement(sql);
            Assert.fail(String.format("String: %s should not be parsed successfully!", sql));
        } catch (Throwable e) {
            Assert.assertTrue(String.format("Actual error hit: %s, expected: Lexical error ...", e.getMessage()),
                    (e instanceof com.splicemachine.db.impl.sql.misc.TokenMgrError) && (e.getMessage().startsWith("Lexical error")));
        }
    }

    @Test
    public void testQuotedString() throws Exception {
        /* slash-star pairs/no pairs enclosed inside quotation mark pair should not be treated as comments,
         * and should not be stripped */
        String sql = "select 'this is a string /*/* test2*/ */ to output' from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        sql = "select 'this is a string /*/* test2 */ to output' from t1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        /* comment outside quotation mark pairs should be stripped off */
        sql = "select 'this is a string /*/* test2 */ to output' from t1 /* now start the where clause */ where a1=1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = "select 'this is a string /*/* test2 */ to output' from t1  where a1=1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

    }

    @Test
    public void testQueryWithoutClosingQuote() throws Exception {
        /* though this is an invalid sql, commentStrip only focus on stripping comments enclosed in the slash-star pairs,
           should CommentStripper should let it go through and leave it to sqlgrammer to report syntax error
         */
        String sql = "select 'test1 * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        /* slash-star appearing after single quotation mark, but the closing quotation mark is missing */
        sql = "select 'this is a string /*/* test2 */ to output from t1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testImportStatement() throws Exception {
        String sql = "call SYSCS_UTIL.IMPORT_DATA ('TPCH1', 'NATION', /* column list */ null, '/var/tmp/tpch/data/1/nation', '|', null, null, null, null, 0, '/var/tmp/BAD', true, null);";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "call SYSCS_UTIL.IMPORT_DATA ('TPCH1', 'NATION',  null, '/var/tmp/tpch/data/1/nation', '|', null, null, null, null, 0, '/var/tmp/BAD', true, null);";;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testStatementWithEscapeForQuotationMark() throws Exception {
        String sql = "select t1.*, '''/*this is a constant*/''' from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testQueryWithSingleLineCommentAndHint() throws Exception {
        // please note, we recognize single-line comment, but don't trim it off
        String sql = "select a1,\n" +
                "b1, -- this is a single comment line, but it won't be trimmed off including /* ??? */ pairs\n" +
                "c1 /* comments enclosed in slash-star pairs here will be trimmed off */,  'constant string' from t1 --splice-properties useSpark=true" +
                "where a1=1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select a1,\n" +
                "b1, -- this is a single comment line, but it won't be trimmed off including /* ??? */ pairs\n" +
                "c1 ,  'constant string' from t1 --splice-properties useSpark=true" +
                "where a1=1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

    }


}
