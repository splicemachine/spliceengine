package com.splicemachine.qpt;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PlanParserTest {

    @Test
    public void testSimpleJoin() throws ParseException {
        String plan =
                "Cursor(n=6,rows=2369593,updateMode=READ_ONLY (1),engine=Spark)\n" +
                        "  ->  ScrollInsensitive(n=5,totalCost=113207.492,outputRows=2369593,outputHeapSize=488.804 MB,partitions=1)\n" +
                        "    ->  ProjectRestrict(n=4,totalCost=59891.557,outputRows=2369593,outputHeapSize=488.804 MB,partitions=1)\n" +
                        "      ->  MergeSortJoin(n=3,totalCost=59891.557,outputRows=2369593,outputHeapSize=488.804 MB,partitions=1,preds=[(O.O_CUSTKEY[4:10] = C.C_CUSTKEY[4:1])])\n" +
                        "        ->  TableScan[ORDERS(2256)](n=2,totalCost=3004,scannedRows=1500000,outputRows=1500000,outputHeapSize=488.804 MB,partitions=1)\n" +
                        "        ->  TableScan[CUSTOMER(2272)](n=1,totalCost=383.5,scannedRows=150000,outputRows=150000,outputHeapSize=21.887 MB,partitions=1)\n";
        List<String> list = Arrays.asList(plan.split("\n"));
        Assert.assertEquals("planID=JAYndyII, shapeID=XLOmQ2wd, scaleID=YZZZZZZZ",
                SQLExecutionPlan.parse(list).toString());

        String expected =
                "Cursor {engineX=, updateMode=READ_ONLY (1), engine=Spark, rows=2369593, n=6}\n" +
                        " ScrollInsensitive {outputHeapSize=488.804 MB, partitions=1, outputRows=2369593, n=5, totalCost=113207.492}\n" +
                        "  ProjectRestrict {outputHeapSize=488.804 MB, partitions=1, outputRows=2369593, n=4, totalCost=59891.557}\n" +
                        "   MergeSortJoin {outputHeapSize=488.804 MB, partitions=1, outputRows=2369593, preds=(O.O_CUSTKEY[4:10] = C.C_CUSTKEY[4:1]), n=3, totalCost=59891.557}\n" +
                        "    TableScan {outputHeapSize=488.804 MB, partitions=1, scannedRows=1500000, outputRows=1500000, table=ORDERS, n=2, totalCost=3004}\n" +
                        "    TableScan {outputHeapSize=21.887 MB, partitions=1, scannedRows=150000, outputRows=150000, table=CUSTOMER, n=1, totalCost=383.5}\n";
        Assert.assertEquals(expected, SQLExecutionPlan.parseTreeNodes(list).toString());
    }

    @Test
    public void testTPCDS20xt() throws ParseException {
        String plan = "Cursor(n=17,rows=100,updateMode=,engine=Spark (forced))\n" +
                "  ->  ScrollInsensitive(n=17,totalCost=152001.779,outputRows=100,outputHeapSize=3.383 MB,partitions=1)\n" +
                "    ->  Limit(n=16,totalCost=151998.697,outputRows=100,outputHeapSize=3.383 MB,partitions=1,fetchFirst=100)\n" +
                "      ->  OrderBy(n=18,totalCost=151998.697,outputRows=100,outputHeapSize=3.383 MB,partitions=1)\n" +
                "        ->  ProjectRestrict(n=14,totalCost=58363.38,outputRows=1144534,outputHeapSize=3.383 MB,partitions=1)\n" +
                "          ->  WindowFunction(n=13,totalCost=2547.41,outputRows=1271705,outputHeapSize=3.383 MB,partitions=1)\n" +
                "            ->  ProjectRestrict(n=12,totalCost=116726.76,outputRows=1144534,outputHeapSize=3.383 MB,partitions=1)\n" +
                "              ->  GroupBy(n=11,totalCost=116726.76,outputRows=1144534,outputHeapSize=3.383 MB,partitions=1)\n" +
                "                ->  ProjectRestrict(n=10,totalCost=5173.983,outputRows=1144534,outputHeapSize=3.383 MB,partitions=1)\n" +
                "                  ->  BroadcastJoin(n=8,totalCost=5173.983,outputRows=1144534,outputHeapSize=3.383 MB,partitions=1,preds=[(CS_ITEM_SK[8:2] = I_ITEM_SK[8:6])])\n" +
                "                    ->  ProjectRestrict(n=7,totalCost=39.757,outputRows=16043,outputHeapSize=3.383 MB,partitions=1,preds=[(I_CATEGORY[6:6] IN (Books                                             ,Jewelry                                           ,Sports    &\n" +
                "                      ->  TableScan[ITEM(58400)](n=6,totalCost=39.65,scannedRows=17825,outputRows=17825,outputHeapSize=3.383 MB,partitions=1)\n" +
                "                    ->  BroadcastJoin(n=4,totalCost=3824.328,outputRows=1144534,outputHeapSize=3.291 MB,partitions=1,preds=[(CS_SOLD_DATE_SK[4:1] = D_DATE_SK[4:4])])\n" +
                "                      ->  TableScan[DATE_DIM(58496)](n=2,totalCost=40.84,scannedRows=18420,outputRows=8737,outputHeapSize=3.291 MB,partitions=1,preds=[(D_DATE[2:2] <= (2001-01-12 + 30)),(D_DATE[2:2] >= 2001-01-12)])\n" +
                "                      ->  ProjectRestrict(n=1,totalCost=2547.41,outputRows=1271705,outputHeapSize=3.638 MB,partitions=1)\n" +
                "                        ->  TableScan[CATALOG_SALES(58080)](n=0,totalCost=2547.41,scannedRows=1271705,outputRows=1271705,outputHeapSize=3.638 MB,partitions=1)\n";
        List<String> list = Arrays.asList(plan.split("\n"));
        Assert.assertEquals("planID=JAEw2VA4, shapeID=XiDC3Llw, scaleID=YZZZZZZZ",
                SQLExecutionPlan.parse(list).toString());
    }


    @Test
    public void testPlanParserNesting() throws ParseException {
        String plan =
                "A(x=0)\n" +
                        "  -> B(x=0)\n" +
                        "    -> C(x=0)\n" +
                        "      -> D(x=0)\n" +
                        "    -> E(x=0)\n" +
                        "      -> F(x=0)\n" +
                        "  -> G(x=0)\n" +
                        "    -> H(x=0)\n" +
                        "  -> I(x=0)\n" +
                        "  -> J(x=0)\n";
        List<String> list = Arrays.asList(plan.split("\n"));
        String expected =
                "A {x=0}\n" +
                        " B {x=0}\n" +
                        "  C {x=0}\n" +
                        "   D {x=0}\n" +
                        "  E {x=0}\n" +
                        "   F {x=0}\n" +
                        " G {x=0}\n" +
                        "  H {x=0}\n" +
                        " I {x=0}\n" +
                        " J {x=0}\n";
        Assert.assertEquals(expected, SQLExecutionPlan.parseTreeNodes(list).toString());
    }
    @Test
    public void testPlanParserBigPlan() throws ParseException {
        int BIG_PLAN_SIZE = 1000;
        ArrayList<String> bigPlan = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        bigPlan.add("Cursor(n=6)");
        sb.append("Cursor {n=6}\n");
        for(int i=1; i<BIG_PLAN_SIZE; i++) {
            bigPlan.add(CommonTest.repeat("  ", i)  + "->  ScrollInsensitive(n=" + i + 5 + ")");
            sb.append(CommonTest.repeat(" ", i)  + "ScrollInsensitive {n=" + i + 5 + "}\n");
        }

        Assert.assertEquals(sb.toString(), SQLExecutionPlan.parseTreeNodes(bigPlan).toString());
        Assert.assertEquals("planID=EAkjsC=X, shapeID=XkjsC=XZ, scaleID=YZZZZZZZ",
                SQLExecutionPlan.parse(bigPlan).toString());
    }

    void expectFails(String plan) {
        List<String> list = Arrays.asList(plan.split("\n"));
        try {
            SQLExecutionPlan.parseTreeNodes(list);
            Assert.fail("expected exception");
        } catch (SQLExecutionPlan.ParseException e) {
            ; // ok
        }
    }

    void expectOK(String plan) throws ParseException {
        List<String> list = Arrays.asList(plan.split("\n"));
        SQLExecutionPlan.parseTreeNodes(list);
    }

    @Test
    public void testPlanParserCornerCases() throws ParseException {
        expectFails("A(x=0)\n" +
                     "       -> B(x=0)\n");

        expectFails("   A(x=0)\n" +
                "       -> B(x=0)\n");

        expectOK("A(x=0)");
        expectOK("sd;lfkh lkj3h5 34b5");
    }
}
