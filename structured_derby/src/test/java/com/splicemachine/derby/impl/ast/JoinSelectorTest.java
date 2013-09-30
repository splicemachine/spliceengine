package com.splicemachine.derby.impl.ast;

import junit.framework.Assert;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.derby.impl.sql.compile.RowResultSetNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * @author P Trolard
 *         Date: 25/09/2013
 */
public class JoinSelectorTest {
    public static ResultSetNode fbt     = new FromBaseTable();
    public static ResultSetNode rows    = new RowResultSetNode();
    public static Predicate pred        = new Predicate();

    public static JoinInfo infoEqui = new JoinInfo(JoinSelector.NLJ, false, false, true, false, false,
                                            Collections.singletonList(pred), Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.singletonList(fbt));

    @Test
    public void basicEquiJoinTest() throws Exception {
        Assert.assertEquals(JoinSelector.MSJ, JoinSelector.chooseStrategy(infoEqui));
    }

    public static JoinInfo infoNonEqui = new JoinInfo(JoinSelector.NLJ, false, false, false, false, false,
                                                Collections.singletonList(pred), Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.singletonList(fbt));

    @Test
    public void basicNonEquiJoinTest() throws Exception {
        Assert.assertEquals(JoinSelector.NLJ, JoinSelector.chooseStrategy(infoNonEqui));
    }


    public static JoinInfo infoCross = new JoinInfo(JoinSelector.NLJ, false, false, false, false, false,
                                            Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.singletonList(fbt));

    @Test
    public void basicCrossJoinTest() throws Exception {
        Assert.assertEquals(JoinSelector.NLJ, JoinSelector.chooseStrategy(infoCross));
    }

    public static JoinInfo infoEquiIndex = new JoinInfo(JoinSelector.NLJ, false, false, true, false, true,
                                            Collections.singletonList(pred), Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.singletonList(fbt));

    @Test
    public void indexTest() throws Exception {
        Assert.assertEquals(JoinSelector.NLJ, JoinSelector.chooseStrategy(infoEquiIndex));
    }

    public static JoinInfo infoRowRS = new JoinInfo(JoinSelector.NLJ, false, false, true, false, false,
                                            Collections.singletonList(pred), Collections.EMPTY_LIST, Collections.EMPTY_LIST, Arrays.asList(rows, rows));

    @Test
    public void rowResultSetTest() throws Exception {
        Assert.assertEquals(JoinSelector.NLJ, JoinSelector.chooseStrategy(infoRowRS));
    }

}
