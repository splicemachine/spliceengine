package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author P Trolard
 *         Date: 19/11/2013
 */
public class MergeJoinRowsTest {

    static final List<String> rightRowStrings = Arrays.asList("1a", "2b", "3b", "4b", "5b", "6c", "7e", "8f");
    static final List<String> leftRowStrings = Arrays.asList("1b", "2b", "3c", "4c", "5c", "6d", "7d", "8f");

    static final Function<String,ExecRow> toRow = new Function<String,ExecRow>() {
        @Override
        public ExecRow apply(String s) {
            ExecRow row = new ValueRow(2);
            row.setColumn(1, new SQLVarchar(s.substring(0,1)));
            row.setColumn(2, new SQLVarchar(s.substring(1)));
            return row;
        }
    };

    static final List<Pair<String,List<String>>> expectedStrings = Arrays.asList(
            Pair.newPair("1b", Arrays.asList("2b", "3b", "4b", "5b")),
            Pair.newPair("2b", Arrays.asList("2b", "3b", "4b", "5b")),
            Pair.newPair("3c", Arrays.asList("6c")),
            Pair.newPair("4c", Arrays.asList("6c")),
            Pair.newPair("5c", Arrays.asList("6c")),
            Pair.newPair("6d", (List<String>)Collections.EMPTY_LIST),
            Pair.newPair("7d", (List<String>)Collections.EMPTY_LIST),
            Pair.newPair("8f", Arrays.asList("8f")));

    static final Function<ExecRow,String> execRowToString = new Function<ExecRow, String>() {
        @Override
        public String apply(ExecRow row) {
            try {
                return row.getColumn(1).toString() + row.getColumn(2).toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    @Test
    public void testPrintJoinedRows() throws Exception {
        IJoinRowsIterator<ExecRow> pairs =
                new MergeJoinRows(
                        Lists.transform(leftRowStrings, toRow).iterator(),
                        Lists.transform(rightRowStrings, toRow).iterator(),
                        new int[]{1},
                        new int[]{1});
        for (Pair<ExecRow,Iterator<ExecRow>> p: pairs){
            System.out.println(String.format("%s: %s", p.getFirst(), Lists.newArrayList(p.getSecond())));
        }
    }

    @Test
    public void testMergeJoinRows() throws Exception {
        IJoinRowsIterator<ExecRow> pairs =
                new MergeJoinRows(
                        Lists.transform(leftRowStrings, toRow).iterator(),
                        Lists.transform(rightRowStrings, toRow).iterator(),
                        new int[]{1},
                        new int[]{1});
        int pos = 0;
        for (Pair<ExecRow,Iterator<ExecRow>> pair: pairs){
            Assert.assertEquals(expectedStrings.get(pos).getFirst(), execRowToString.apply(pair.getFirst()));
            Assert.assertEquals(expectedStrings.get(pos).getSecond(),
                    Lists.transform(Lists.newArrayList(pair.getSecond()), execRowToString));
            pos++;
        }
    }
}
