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

    @Test
    public void testQueryWithoutClosingCommentMark() throws Exception {
        /* all characters after the opening comment mark will be skipped until a closing comment mark is seen. It is possible that an
           end of string is seen without the closing comment mark, in that case, we will output a string ignorig all characters after the opening
           comment mark
         */
        String sql = "select /* test1 * from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select ";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        /* nested comments without matching pairs */
        sql = "select /* test1 /* nested comment */ from t1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testSingleQuotedString() throws Exception {
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
    public void testQueryWithoutClosingSingleQuote() throws Exception {
        /* though this is an invalid sql, commentStrip only focus on stripping comments enclosed in the slash-star pairs,
           CommentStripper should let it go through and leave it to sqlgrammer to report syntax error
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
    public void testStatementWithEscapeForSingleQuotationMark() throws Exception {
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

    @Test
    public void testDoubleQuotedString() throws Exception {
        /* slash-star pairs/no pairs enclosed inside quotation mark pair should not be treated as comments,
         * and should not be stripped */
        String sql = "create table \"it's a strange table name with /*/* comment*/ */\" (\"col/*1*/\" int, col2 int);";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        /* comment outside quotation mark pairs should be stripped off */
        sql = "select \"col/*1*/\", /* this comment should be stripped */ col2 from t1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = "select \"col/*1*/\",  col2 from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        /* double quote enclosed in comment mark pair will be treated as part of the comment and should be stripped off */
        sql = "select /* this comment including the special character \" and \" pair should be stripped */ col2 from t1;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = "select  col2 from t1;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testQueryWithoutClosingDoubleQuote() throws Exception {
        /* though this is an invalid sql, commentStrip only focus on stripping comments enclosed in the slash-star pairs,
           CommentStripper should let it go through and leave it to sqlgrammer to report syntax error
         */
        String sql = "select \"col/*1*/ from t1;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = sql;
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

    @Test
    public void testQueryWithMixtureOfSingleAndDoubleQuote() throws Exception {
        /* if single quote comes first, everything follows(including double quote or comment mark will be treated as
           part of the quoted string until the paired sinle quote appears
         */
        String sql = "select '\"col/*1*/' from t1 /*this is comment that should be stripped*/;";
        String sqlTrimmed = commentStripper.stripStatement(sql);
        String expected = "select '\"col/*1*/' from t1 ;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);

        /* similarly, if a double quote comes first, everything follows(including single quote or comment mark will be treated as
           part of the double-quoted string until the paired double quote appears
         */
        sql = "select \"col'1\" from t1 /*this is comment that should be stripped*/;";
        sqlTrimmed = commentStripper.stripStatement(sql);
        expected = "select \"col'1\" from t1 ;";
        Assert.assertEquals("sqlTrimmed: " + sqlTrimmed + ", expected: " + expected, expected, sqlTrimmed);
    }

}
