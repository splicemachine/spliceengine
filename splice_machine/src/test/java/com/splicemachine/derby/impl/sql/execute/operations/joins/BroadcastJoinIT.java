/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Date: 5/20/15
 */


@RunWith(Parameterized.class)
@Category(LongerThanTwoMinutes.class)
public class BroadcastJoinIT extends SpliceUnitTest {

    private Boolean useSpark;
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public BroadcastJoinIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(BroadcastJoinIT.class.getSimpleName().toUpperCase());

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(schemaWatcher.schemaName);

    public static final SpliceTableWatcher a= new SpliceTableWatcher("A",schemaWatcher.schemaName,"(c1 int, c2 int)");
    public static final SpliceTableWatcher b= new SpliceTableWatcher("B",schemaWatcher.schemaName,"(c2 int,c3 int)");
    public static final SpliceTableWatcher date_dim= new SpliceTableWatcher("date_dim",schemaWatcher.schemaName,"(d_year int, d_qoy int)");
    public static final SpliceTableWatcher t1= new SpliceTableWatcher("t1",schemaWatcher.schemaName,"(a1 int, b1 int, c1 int)");
    public static final SpliceTableWatcher t2= new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(a2 int, b2 int)");
    public static final SpliceTableWatcher t11= new SpliceTableWatcher("t11",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher t22= new SpliceTableWatcher("t22",schemaWatcher.schemaName,"(a int, b int, primary key(a))");
    public static final SpliceTableWatcher t33= new SpliceTableWatcher("t33",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher s1= new SpliceTableWatcher("s1",schemaWatcher.schemaName,"(a1 int, b1 char(2), c1 char(10), d1 char(10), e1 char(20))");
    public static final SpliceTableWatcher s2= new SpliceTableWatcher("s2",schemaWatcher.schemaName,"(a2 int, b2 boolean, c2 date, d2 time, e2 timestamp)");
    public static final SpliceTableWatcher s3 = new SpliceTableWatcher("s3", schemaWatcher.schemaName, "(num1 dec(31,1), num2 double, num3 float, num4 real, num5 int)");

    public static final SpliceTableWatcher at = new SpliceTableWatcher("AT", schemaWatcher.schemaName, "(auftrgeb_gf# char(36), auftrstatus char(4), auftrart char(2))");
    public static final SpliceTableWatcher dr = new SpliceTableWatcher("DR", schemaWatcher.schemaName, "(domname varchar(18), domregel varchar(65), domb# char(36))");
    public static final SpliceTableWatcher x3 = new SpliceTableWatcher("X3", schemaWatcher.schemaName, "(domwliste varchar(1900), loganwber char(8), domb# char(36))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher(BroadcastJoinIT.class.getSimpleName().toUpperCase());
    private static boolean isMemPlatform = false;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(a)
            .around(b)
            .around(date_dim)
            .around(t1)
            .around(t2)
            .around(t11)
            .around(t22)
            .around(t33)
            .around(s1)
            .around(s2)
            .around(s3)
            .around(at)
            .around(dr)
            .around(x3)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + s3 + "(num1,num2,num3,num4,num5) values (?,?,?,?,?)")) {
                        ps.setBigDecimal(1, BigDecimal.ONE);
                        ps.setDouble(2, Double.valueOf(1));
                        ps.setFloat(3, Float.valueOf(1));
                        ps.setFloat(4, Float.valueOf(1));
                        ps.setInt(5, 1);
                        ps.execute();
                        ps.setBigDecimal(1, BigDecimal.valueOf(-1.1));
                        ps.setDouble(2, Double.valueOf("-1.1"));
                        ps.setDouble(3, Double.valueOf("-1.1"));
                        ps.setFloat(4, Float.valueOf("-1.1"));
                        ps.setInt(5, -1);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+a+"(c1,c2) values (?,?)")){
                        ps.setInt(1,1);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,3);ps.setInt(2,3);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+b+"(c2,c3) values (?,?)")){
                        ps.setInt(1,1);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,3);ps.setInt(2,3);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + date_dim + "(d_year, d_qoy) values (?,?)")) {
                        ps.setInt(1,1999);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,1999);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,1999);ps.setInt(2,3);ps.execute();
                        ps.setInt(1,1999);ps.setInt(2,4);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,3);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,4);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,3);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,4);ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + t1 + "(a1, b1, c1) values (?,?,?)")) {
                        ps.setInt(1, 1);ps.setInt(2, 2);ps.setInt(3, 3);ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+t2 +"(a2,b2) values (?,?)")){
                        ps.setInt(1,1);ps.setInt(2,22);ps.execute();
                        ps.setInt(1,4);ps.setInt(2,44);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + s1 + "(a1, b1, c1, d1, e1) values (?,?,?,?,?)")) {
                        ps.setInt(1, 1);ps.setString(2, "1");ps.setString(3, "2018-11-11");
                        ps.setString(4, "13:00:00");ps.setString(5, "2018-12-24 11:11:11");ps.execute();
                    } catch (Exception e) {
                throw new RuntimeException(e);
            }
            }}).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + s2 + "(a2, b2, c2, d2, e2) values (?,?,?,?,?)")) {
                        ps.setInt(1, 1);ps.setBoolean(2,true);ps.setDate(3, Date.valueOf("2018-11-11"));
                        ps.setTime(4, Time.valueOf("13:00:00"));ps.setTimestamp(5, Timestamp.valueOf("2018-12-24 11:11:11"));ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
            }}).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + at + " values (?,?,?)")) {
                        ps.setString(1,"c37809fc-dbc0-11dc-bb9d-00110a38ff8a");ps.setString(2,"GMST");ps.setString(3, "GM");ps.execute();
                        ps.setString(1,"7ee72ba2-c983-11dc-9bb2-000bcd3dea92");ps.setString(2,"GMEO");ps.setString(3, "GM");ps.execute();
                        ps.setString(1,"b162ede2-d38f-11d9-8000-010157aa0000");ps.setString(2,"GMEO");ps.setString(3, "AM");ps.execute();
                        ps.setString(1,"8ea16564-026c-11e7-bd3a-0050568766a6");ps.setString(2,"GTER");ps.setString(3, "BM");ps.execute();
                        ps.setString(1,"4bbcc862-0b14-11d9-8000-010157aa0000");ps.setString(2,"GMEO");ps.setString(3, "CM");ps.execute();
                        ps.setString(1,"58c2e22b-ce73-11dc-bcfc-00110a3923a8");ps.setString(2,"GMEO");ps.setString(3, "DM");ps.execute();
                        ps.setString(1,"35ea068a-91cd-11dc-8ecb-001321040bb4");ps.setString(2,"GMST");ps.setString(3, "EM");ps.execute();
                        ps.setString(1,"efd55af2-c38f-11d9-8000-010157aa0000");ps.setString(2,"GMST");ps.setString(3, "FM");ps.execute();
                        ps.setString(1,"a09c7d39-cd75-11dc-b6e8-000bcd3dea92");ps.setString(2,"GMST");ps.setString(3, "HM");ps.execute();
                        ps.setString(1,"c8cd5a3a-e199-11da-8000-010157aa0000");ps.setString(2,"GMST");ps.setString(3, "IM");ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + dr + " values (?,?,?)")) {
                        ps.setString(1,"AUFTRAGSART");ps.setString(2,"SCHLIESSEN_AUFTRAG");ps.setString(3, "1205686f-b899-11e6-9f7d-9cb654a0f4f6");ps.execute();
                        ps.setString(1,"KENNZEICHEN_JN");ps.setString(2,"SME_BETRIEBSARTEN_KZ_HP");ps.setString(3, "00a8441a-ab9e-11e8-b9d5-9cb654a0637d");ps.execute();
                        ps.setString(1,"BUCHUNGSART");ps.setString(2,"BUCHUNGSART_SK");ps.setString(3, "01006252-ebe2-11de-a13d-6f0f671c0089");ps.execute();
                        ps.setString(1,"RV_BQUEBSTEUERUNG");ps.setString(2,"FLOTTENNR_BQUEBST");ps.setString(3, "018b9c38-7a4b-11e4-9727-9cb654a1d902");ps.execute();
                        ps.setString(1,"SPARTE_EIGEN");ps.setString(2,"XLOB_PRODPAKET_ZU_SPARTE");ps.setString(3, "01be147b-ded6-11e5-aab4-9cb654a4c4c6");ps.execute();
                        ps.setString(1,"LSCHICHTZUSBEZ");ps.setString(2,"LSCHZUS_ZUSBEZ");ps.setString(3, "0228b814-440f-11e6-92cf-9cb654a1cb9b");ps.execute();
                        ps.setString(1,"GBPART");ps.setString(2,"AUFTRAGSART_ZU_GBP_ART");ps.setString(3, "02750d20-ac38-11de-b23d-6f0f671c00a3");ps.execute();
                        ps.setString(1,"XPS_PGEN_B");ps.setString(2,"XPS_REINIT_PGEN");ps.setString(3, "02b6341a-25df-11e8-9b2d-901b0e1c176e");ps.execute();
                        ps.setString(1,"AZBZUSDART");ps.setString(2,"ABZUEGE_SONDERZAHLUNG");ps.setString(3, "030908eb-d705-11e5-bf03-9cb654a0f4f6");ps.execute();
                        ps.setString(1,"SPARTE_EIGEN");ps.setString(2,"PPMKFZ");ps.setString(3, "0313241a-3f20-11e9-a35c-9cb654a17a67");ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + x3 + " values (?,?,?)")) {
                        ps.setString(1,"GM");ps.setString(2,"SCHADEN ");ps.setString(3, "1205686f-b899-11e6-9f7d-9cb654a0f4f6");ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            ;

    private static TestConnection conn;
    private static final int numIterations = 30;

    @BeforeClass
    public static void setUpClass() throws Exception{
        conn = classWatcher.getOrCreateConnection();
        isMemPlatform = isMemPlatform(classWatcher);
    }

    public static void createData(Connection conn, String schemaName) throws Exception {
        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int)")
                .withInsert("insert into t3 values(?,?,?,?)")
                .withRows(rows(
                        row(1095236,0,37770,0)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 numeric(31,0) not null, d4 numeric(31,0) not null, primary key(c4,d4))")
                .withInsert("insert into t4 values(?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1),
                        row(2,2,2,2),
                        row(3,3,3,3),
                        row(4,4,4,4),
                        row(5,5,5,5),
                        row(6,6,6,6),
                        row(7,7,7,7),
                        row(8,8,8,8),
                        row(9,9,9,9),
                        row(10,10,10,10)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            classWatcher.executeUpdate(SpliceUnitTest.format("insert into t4 select a4, b4,c4+%d, d4 from t4", factor));
            factor = factor * 2;
        }

        new TableCreator(conn)
        .withCreate("create table tab1 (a int, b int, primary key(a))")
        .withInsert("insert into tab1 values(?,?)")
        .withRows(rows(
        row(1,1),
        row(2,2),
        row(3,3),
        row(4,4),
        row(5,5),
        row(6,6),
        row(7,7),
        row(8,8),
        row(9,9),
        row(10,10)))
        .create();

        new TableCreator(conn)
        .withCreate("create table tab2 (a int, b int, primary key(a))")
        .withInsert("insert into tab2 values(?,?)")
        .withRows(rows(
        row(1,1),
        row(2,2),
        row(3,3),
        row(4,4),
        row(5,5),
        row(6,6),
        row(7,7),
        row(8,8),
        row(9,9),
        row(10,10)))
        .create();

        new TableCreator(conn)
        .withCreate("create table tab3 (a int, b int)")
        .withIndex("create index ix on tab3(a)")
        .withInsert("insert into tab3 values(?,?)")
        .withRows(rows(
        row(5,5),
        row(6,6),
        row(7,7),
        row(8,8),
        row(9,9),
        row(10,10)))
        .create();

        factor = 10;
        for (int i = 1; i <= 8; i++) {
            classWatcher.executeUpdate(SpliceUnitTest.format("insert into tab1 select a+%d,b from tab1", factor));
            factor = factor * 2;
        }
        ResultSet rs = classWatcher.executeQuery("analyze table tab1");
        rs.close();

        new TableCreator(conn)
        .withCreate("create table tab4 (a int, b varchar(10))")
        .withInsert("insert into tab4 values(?,?)")
        .withRows(rows(
        row(2, "abc"),
        row(3, "abcdefghi")
        )).create();

        new TableCreator(conn)
        .withCreate("create table tab5 (a int, b varchar(10))")
        .withInsert("insert into tab5 values(?,?)")
        .withRows(rows(
        row(3, "bc")
        )).create();

        new TableCreator(conn)
        .withCreate("create table tab6 (a int, b varchar(10))")
        .withInsert("insert into tab6 values(?,?)")
        .withRows(rows(
        row(3, "c"),
        row(3, "ce")
        )).create();

        classWatcher.executeUpdate("insert into t11 values (2,2)");
        classWatcher.executeUpdate("insert into t11 values (3,3)");
        classWatcher.executeUpdate("insert into t11 values (4,4)");
        classWatcher.executeUpdate("insert into t11 values (5,5)");
        classWatcher.executeUpdate("insert into t11 values (6,6)");
        classWatcher.executeUpdate("insert into t11 values (7,7)");
        classWatcher.executeUpdate("insert into t11 values (8,8)");
        classWatcher.executeUpdate("insert into t11 values (9,9)");
        classWatcher.executeUpdate("insert into t11 values (10,10)");

        classWatcher.executeUpdate("insert into t11 select a+10,b+10 from t11");
        classWatcher.executeUpdate("insert into t11 select a+20,b+20 from t11");
        classWatcher.executeUpdate("insert into t11 select a+40,b+40 from t11");
        classWatcher.executeUpdate("insert into t11 select a+80,b+80 from t11");
        classWatcher.executeUpdate("insert into t11 select a+160,b+160 from t11");
        classWatcher.executeUpdate("insert into t11 select a+320,b+320 from t11");
        classWatcher.executeUpdate("insert into t11 select a+640,b+640 from t11");
        classWatcher.executeUpdate("insert into t11 select a+1280,b+1280 from t11");
        classWatcher.executeUpdate("insert into t11 select a+2560,b+1280 from t11");
        classWatcher.executeUpdate("insert into t11 select a+5120,b+1280 from t11");
        classWatcher.executeUpdate("insert into t11 select a+10240,b+1280 from t11");
        classWatcher.executeUpdate("insert into t11 select a+20480,b+1280 from t11");

        classWatcher.executeUpdate("insert into t11 values (1,1)");
        classWatcher.executeUpdate("insert into t33 values (1,1)");
        classWatcher.executeUpdate("insert into t22 select * from t11");

        classWatcher.executeUpdate(format("call syscs_util.syscs_flush_table('%s', 'T11')", schemaName));
        classWatcher.executeUpdate(format("call syscs_util.syscs_flush_table('%s', 'T22')", schemaName));

        classWatcher.executeQuery(format("analyze table T11", schemaName));
        classWatcher.executeQuery(format("analyze table T22", schemaName));
        classWatcher.executeQuery(format("analyze table T33", schemaName));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }
    
    @Test
    public void testNumericJoinColumns() throws Exception {

        String [] joinTypes = {"NestedLoop", "SortMerge", "Broadcast", "Cross" };
        for (String strategy:joinTypes) {
            String expected = "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |";

            String expected2 = "1 |\n" +
            "----\n" +
            " 1 |";

            String sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num1=tab2.num2", strategy, useSpark
            );

            ResultSet rs = classWatcher.executeQuery(sqlText);
            String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num1=tab2.num3", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num1=tab2.num4", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num1=tab2.num5", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected2 + "\n,actual result: " + resultString, expected2, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num3=tab2.num1", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num3=tab2.num2", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num3=tab2.num4", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
            rs.close();

            sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=%s,useSpark=%s \n" +
            " where tab1.num3=tab2.num5", strategy, useSpark
            );

            rs = classWatcher.executeQuery(sqlText);
            resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected2 + "\n,actual result: " + resultString, expected2, resultString);
            rs.close();
        }
    }

    @Test
    @Ignore("Takes a super long time to work, and then knocks over the region server with an OOM")
    public void testBroadcastJoinDoesNotCauseRegionServerToCollapse() throws Exception{
        String querySQL = format("select count(*) from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                " "+a+" l,"+ b+" r --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s)\n" +
                " where l.c2 = r.c2", useSpark);
        String insertSQL = "insert into "+ b+"(c2,c3) select * from "+b;
        try(PreparedStatement queryStatement = conn.prepareStatement(querySQL)){
            try(PreparedStatement insertStatement = conn.prepareStatement(insertSQL)){
                for(int i=0;i<numIterations;i++){
                    insertStatement.execute();
                    try(ResultSet rs = queryStatement.executeQuery()){
                        Assert.assertTrue("Weird: count(*) did not return the correct number of records!",rs.next());
                    }
                }
            }
        }
    }

    @Test
    public void testInClauseBroadCastJoin() throws Exception {
        String sqlText = format("select count(*) from " + date_dim + " d --SPLICE-PROPERTIES useSpark = %s \n" +
                "where d.d_qoy in (select d_qoy from " + date_dim + " )", useSpark) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(*) returned incorrect number of rows:", (c == 12));
        rs.close();

        sqlText = format("with ws_wh as (select * from " + date_dim + " dim " +
        " where dim.d_qoy > 2 ) " +
        " select count(*) " +
        "   from  "+ date_dim + " d --SPLICE-PROPERTIES useSpark = %s \n" +
        "   where d.d_qoy in (select d_qoy from ws_wh)", useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(*) returned incorrect number of rows:", (c == 6));
        rs.close();
    }

    @Test
    public void testRightOuterJoinViaBroadCastJoin() throws Exception {
        String sqlText = format("select a1,a2,b1,b2,c1 from " + t1 + " right join " + t2 +" --SPLICE-PROPERTIES useSpark = %s \n" +
                "on a1 = a2 order by 2 desc", useSpark) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int a1 = rs.getInt(1);
        Assert.assertTrue("incorrect result:", rs.wasNull());
        int a2 = rs.getInt(2);
        Assert.assertTrue("incorrect result:", (a2==4));
        rs.close();
    }

    @Test
    public void testBroadCastJoinWithIntToNumericCast() throws Exception {
        String sqlText = format("select c4 from --splice-properties joinOrder=fixed\n" +
                "t4 t --splice-properties useSpark=%s\n" +
                ",t3 c --splice-properties joinStrategy=broadcast\n" +
                "where c.c3=t.c4 and t.c4 >=37770 and t.c4 <37771", useSpark);
        String expected = "C4   |\n" +
                "-------\n" +
                "37770 |";

        ResultSet rs = classWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testInequalityBroadCastJoin() throws Exception {
        String sqlText = format("select count(*) from --splice-properties joinOrder=fixed\n" +
        "tab1 tt1 --splice-properties useSpark=%s\n" +
        "inner join tab1 tt2 --splice-properties useSpark=%s,joinStrategy=broadcast\n" +
        "on tt1.a between tt2.a - 5 and tt2.a + 5 and tt1.a in (10, 20, 30, 40)", useSpark, useSpark);


        ResultSet rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 44, rs.getInt(1));

        sqlText = format("select count(*) from\n" +
        "tab1 tt1 --splice-properties useSpark=%s\n" +
        "inner join tab1 tt2 --splice-properties useSpark=%s,joinStrategy=broadcast\n" +
        "on tt1.a between tt2.a - 5 and tt2.a + 5 and tt1.a in (10, 20, 30, 40)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 44, rs.getInt(1));

        sqlText = format("explain select count(*) from\n" +
        "tab1 tt1 --splice-properties useSpark=%s\n" +
        "inner join tab1 tt2 --splice-properties useSpark=%s\n" +
        "on tt1.a between tt2.a - 5 and tt2.a + 5 and tt1.b in (10, 20, 30, 40)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);

        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        boolean broadcastJoinPresent = explainPlanText.contains("BroadcastJoin");
        // Mem platform cannot test useSpark selection of broadcast join,
        // so just test if it's not picked when useSpark=false.
        if (!useSpark)
            Assert.assertTrue("Query is not expected to pick BroadcastJoin,, the current plan is: " + explainPlanText, !broadcastJoinPresent);


        sqlText = format("explain select count(*) from\n" +
        "tab1 tt1 --splice-properties useSpark=%s\n" +
        "inner join tab1 tt2 --splice-properties useSpark=%s,joinStrategy=nestedloop\n" +
        "on tt1.a between tt2.a - 5 and tt2.a + 5 and tt1.a in (10, 20, 30, 40)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        boolean nestedLoopJoinPresent = explainPlanText.contains("NestedLoop");
        // To be enabled by SPLICE-2159, once it's fixed.
        //Assert.assertTrue("Query is expected to pick nested loop join, the current plan is: " + explainPlanText, nestedLoopJoinPresent);

        sqlText = format("select count(*) from --splice-properties joinOrder=fixed\n" +
        "tab2 --splice-properties useSpark=%s\n" +
        "left outer join tab3 --splice-properties useSpark=%s,joinStrategy=broadcast\n" +
        "on tab2.a between tab3.a - 1 and tab3.a and tab2.a in (1, 3, 5)", useSpark, useSpark);


        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 11, rs.getInt(1));

        sqlText = format("select count(*) from --splice-properties joinOrder=fixed\n" +
        "tab2 --splice-properties useSpark=%s,joinStrategy=broadcast\n" +
        "right outer join tab3 --splice-properties useSpark=%s\n" +
        "on tab2.a between tab3.a - 1 and tab3.a and tab2.a in (1, 3, 5)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 6, rs.getInt(1));

        sqlText = format("select count(*) from\n" +
        "tab1 --splice-properties useSpark=%s\n" +
        "where tab1.a in (1, 3, 5) and\n" +
        "        exists (select * from tab3 --splice-properties useSpark=%s\n" +
        "                where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 1, rs.getInt(1));

        sqlText = format("select count(*) from\n" +
        "tab1 --splice-properties useSpark=%s\n" +
        "where tab1.a in (1, 3, 5) and\n" +
        "    not exists (select * from tab3 --splice-properties useSpark=%s\n" +
        "                where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 2, rs.getInt(1));

        sqlText = format("select count(*) from\n" +
        "        tab1 --splice-properties useSpark=%s\n" +
        "        where tab1.b in\n" +
        "         (select b from tab3 --splice-properties useSpark=%s\n" +
        "                   where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 6, rs.getInt(1));

        sqlText = format("select count(*) from\n" +
        "        tab1 --splice-properties useSpark=%s\n" +
        "        where tab1.b not in\n" +
        "         (select b from tab3 --splice-properties useSpark=%s\n" +
        "                   where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 2554, rs.getInt(1));

        rs.close();
    }

    @Test
    public void testCharBooleanColumnsBroadCastJoin() throws Exception {
        String sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s1.b1 = s2.b2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s2.b2 = s1.b1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testCharDateColumnsBroadCastJoin() throws Exception {
        String sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s1.c1 = s2.c2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s2.c2 = s1.c1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testIllegalFormatCharDateColumnsBroadCastJoin() throws Exception {
        String sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s1.b1 = s2.c2" , useSpark
        ) ;
        try (ResultSet rs = classWatcher.executeQuery(sqlText)) {
            Assert.fail("Exception not thrown");
        } catch (SQLDataException e) {
            assertEquals("22007", e.getSQLState());
        }
    }

    @Test
    public void testComparableCharColumnsBroadCastJoin() throws Exception {
        String sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s1.b1 = s2.a2" , useSpark
        ) ;

        // The following query with CHAR/INT comparison should not throw an error.
        try (ResultSet rs = classWatcher.executeQuery(sqlText)) {

        }
    }

    @Test
    public void testCharTimeColumnsBroadCastJoin() throws Exception {
        String sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s1.d1 = s2.d2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s2.d2 = s1.d1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testCharTimestampColumnsBroadCastJoin() throws Exception {
        String sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s1.e1 = s2.e2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from " + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on s2.e2 = s1.e1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testSubstrInJoinPredicate() throws Exception {
        String sqlTexts[] = {
                format("select * from tab4 --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s %n" +
                        "inner join tab5 on tab5.b = SUBSTR(tab4.b, 2) %n" +
                        "left join tab6 on tab6.b = SUBSTR(tab4.b, 3)", useSpark),
                format("select * from tab4 --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s %n" +
                        "inner join tab5 on tab5.b = SUBSTR('abc', tab4.a) %n" +
                        "left join tab6 on tab6.b = SUBSTR('bc', tab4.a)", useSpark),
                format("select * from tab4 --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s %n" +
                        "inner join tab5 on tab5.b = SUBSTR('bcefgh', 1, tab4.a) %n" +
                        "left join tab6 on tab6.b = SUBSTR('cefgh', 1, tab4.a)", useSpark)
        };

        String expecteds[] = {
                "A | B  | A | B | A | B |\n" +
                "-------------------------\n" +
                " 2 |abc | 3 |bc | 3 | c |",
                "A | B  | A | B | A | B |\n" +
                "-------------------------\n" +
                " 2 |abc | 3 |bc | 3 | c |",
                "A | B  | A | B | A | B |\n" +
                "-------------------------\n" +
                " 2 |abc | 3 |bc | 3 |ce |"
        };
        for (int i = 0; i < expecteds.length; ++i) {
            String sqlText = sqlTexts[i];
            String expected = expecteds[i];
            try (ResultSet rs = classWatcher.executeQuery(sqlText)) {
                String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
                assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n, actual result: " + resultString, expected, resultString);
            }
        }
    }

    @Test
    public void testCaseInJoinPredicate() throws Exception {
        String[] sqlTexts = {
                format("select * from tab2 inner join tab3 --splice-properties joinStrategy=broadcast, useSpark=%s\n" +
                       "on case when tab2.b=3 then 5 else 0 end = tab3.b", useSpark),

                format("select * from tab2 inner join tab3 --splice-properties joinStrategy=broadcast, useSpark=%s\n" +
                       "on case when tab2.b is null then 5 else 9 end = tab3.b order by 1,2,3,4", useSpark),
        };
        String[] expecteds = {
                "A | B | A | B |\n" +
                "----------------\n" +
                " 3 | 3 | 5 | 5 |",

                "A | B | A | B |\n" +
                "----------------\n" +
                " 1 | 1 | 9 | 9 |\n" +
                " 2 | 2 | 9 | 9 |\n" +
                " 3 | 3 | 9 | 9 |\n" +
                " 4 | 4 | 9 | 9 |\n" +
                " 5 | 5 | 9 | 9 |\n" +
                " 6 | 6 | 9 | 9 |\n" +
                " 7 | 7 | 9 | 9 |\n" +
                " 8 | 8 | 9 | 9 |\n" +
                " 9 | 9 | 9 | 9 |\n" +
                "10 |10 | 9 | 9 |"
        };
        for (int i = 0; i < sqlTexts.length; ++i) {
            String sqlText = sqlTexts[i];
            String expected = expecteds[i];
            try (ResultSet rs = classWatcher.executeQuery(sqlText)) {
                String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
                assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n, actual result: " + resultString, expected, resultString);
            }
        }
    }

    @Test
    public void testBroadcastJoinInNonFlattenedCorrelatedSubquery_1() throws Exception {
        String sqlText = "select * from tab4 where a in (select tab5.a from tab6, tab5 --splice-properties joinStrategy=broadcast\n" +
                "where tab5.a=tab6.a and tab4.a=tab5.a)";
        String expected = "A |    B     |\n" +
                "---------------\n" +
                " 3 |abcdefghi |";
        try (ResultSet rs = classWatcher.executeQuery(sqlText)) {
            String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n, actual result: " + resultString, expected, resultString);
        }
    }

    @Test
    public void testBroadcastJoinInNonFlattenedCorrelatedSubquery_2() throws Exception {
        String query = "SELECT auftrgeb_gf#, auftrart " +
                " FROM " + at +
                " WHERE " +
                "   EXISTS (SELECT 1 " +
                "           FROM " + dr + ", " + x3 + " --splice-properties joinStrategy=%s, useSpark=%s\n" +
                "           WHERE " +
                "                  " + dr + ".domb# = " + x3 + ".domb# AND " +
                "      ( Locate(" + at + ".auftrart, " + x3 + ".domwliste) > 0 ) )";

        String expected =
                "AUFTRGEB_GF#             |AUFTRART |\n" +
                "------------------------------------------------\n" +
                "7ee72ba2-c983-11dc-9bb2-000bcd3dea92 |   GM    |\n" +
                "c37809fc-dbc0-11dc-bb9d-00110a38ff8a |   GM    |";

        try (ResultSet rs = methodWatcher.executeQuery(String.format(query, "broadcast", useSpark.toString()))) {
            String resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertEquals("expected result: " + expected + "\n, actual result: " + resultString, expected, resultString);
        }
        try (ResultSet rs = methodWatcher.executeQuery(String.format(query, "cross", "true"))) {
            String resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertEquals("expected result: " + expected + "\n, actual result: " + resultString, expected, resultString);
        }
    }

    @Test
    public void testOuterBroadCastJoinWithSingleTableJoinPreds() throws Exception {
        String sqlText = format("select * from " + t1 +
                " left outer join " + t2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on t2.a2 = 1" , useSpark
        );
        String expected = "A1 |B1 |C1 |A2 |B2 |\n" +
                            "--------------------\n" +
                            " 1 | 2 | 3 | 1 |22 |";
        testQuery(sqlText, expected, classWatcher);
        String explainQuery = "explain " + sqlText;
        rowContainsQuery(3, explainQuery, "BroadcastLeftOuterJoin", methodWatcher);

        sqlText = format("select * from " + t1 +
                " left outer join " + t2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on t2.a2 = 0" , useSpark);
        expected = "A1 |B1 |C1 | A2  | B2  |\n" +
                "------------------------\n" +
                " 1 | 2 | 3 |NULL |NULL |";
        testQuery(sqlText, expected, classWatcher);

        sqlText = format("select * from " + t1 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                        " right outer join " + t2 +
                        " on t2.a2 = 0" , useSpark);
        expected = "A1  | B1  | C1  |A2 |B2 |\n" +
                    "--------------------------\n" +
                    "NULL |NULL |NULL | 1 |22 |\n" +
                    "NULL |NULL |NULL | 4 |44 |";
        testQuery(sqlText, expected, classWatcher);
        explainQuery = "explain " + sqlText;
        rowContainsQuery(4, explainQuery, "BroadcastLeftOuterJoin", methodWatcher);

        sqlText = format("select * from " + t1 +
                        " full outer join " + t2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                        " on t2.a2 = 0" , useSpark);
        expected = "A1  | B1  | C1  | A2  | B2  |\n" +
                    "------------------------------\n" +
                    "  1  |  2  |  3  |NULL |NULL |\n" +
                    "NULL |NULL |NULL |  1  | 22  |\n" +
                    "NULL |NULL |NULL |  4  | 44  |";
        testQuery(sqlText, expected, classWatcher);
        explainQuery = "explain " + sqlText;
        rowContainsQuery(3, explainQuery, "BroadcastFullOuterJoin", methodWatcher);

        sqlText = format("select * from " + t1 +
                " left outer join " + t2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on t2.a2 = 1 or t1.a1 = 1" , useSpark
        );
        expected = "A1 |B1 |C1 |A2 |B2 |\n" +
                    "--------------------\n" +
                    " 1 | 2 | 3 | 1 |22 |\n" +
                    " 1 | 2 | 3 | 4 |44 |";
        testQuery(sqlText, expected, classWatcher);
        explainQuery = "explain " + sqlText;
        rowContainsQuery(3, explainQuery, "BroadcastLeftOuterJoin", methodWatcher);

        sqlText = format("select * from " + t1 +
                " left outer join " + t2 + " --SPLICE-PROPERTIES joinStrategy=BROADCAST,useSpark=%s \n" +
                " on 1=1" , useSpark
        );
        testQuery(sqlText, expected, classWatcher);
        explainQuery = "explain " + sqlText;
        rowContainsQuery(3, explainQuery, "BroadcastLeftOuterJoin", methodWatcher);
    }

    @Test
    public void testDisableNestedLoopJoinOnSpark() throws Exception {
        if (isMemPlatform && useSpark)
            return;
        String sqlText = "select max(t11.a+1) from\n" +
                            "(select max(a.a) from t11 a\n" +
                            "inner join t22 b --splice-properties useSpark=" + useSpark + "\n" +
                            "on a.a=b.a inner join t11 c\n" +
                            "on b.a=c.a inner join t22 d\n" +
                            "on c.a=d.a inner join t22 e\n" +
                            "on d.a=e.a inner join t22 f\n" +
                            "on e.a=f.a inner join t22 g\n" +
                            "on f.a=g.a inner join t22 h\n" +
                            "on g.a=h.a inner join t22 i\n" +
                            "on h.a=i.a inner join t22 j\n" +
                            "on i.a=j.a inner join t22 k\n" +
                            "on j.a=k.a inner join t22 l\n" +
                            "on k.a=l.a\n" +
                            "and\n" +
                            "l.b >= (select a from t33)\n" +
                            "where a.a between 1 and 900\n" +
                            "group by a.b) myTab(a) \n" +
                            ",t11 \n" +
                            "where t11.a between 1 and 90 and t11.b = myTab.a";
        String expected = "1 |\n" +
                          "----\n" +
                          "91 |";
        // The query runs too long on control.
        if (useSpark)
            testQuery(sqlText, expected, methodWatcher);
        String explainQuery = "explain " + sqlText;
        if (useSpark)
            testQueryDoesNotContain(explainQuery, "NestedLoopJoin", methodWatcher, true);
        else
            testQueryContains(explainQuery, "NestedLoopJoin", methodWatcher, true);

        // Make sure we can still hint nested loop join.
        if (useSpark) {
            explainQuery = "explain select max(t11.a+1) from\n" +
                            "(select max(a.a) from t11 a\n" +
                            "inner join t22 b --splice-properties joinStrategy=nestedloop, useSpark=" + useSpark + "\n" +
                            "on a.a=b.a inner join t11 c\n" +
                            "on b.a=c.a inner join t22 d\n" +
                            "on c.a=d.a inner join t22 e\n" +
                            "on d.a=e.a inner join t22 f\n" +
                            "on e.a=f.a inner join t22 g\n" +
                            "on f.a=g.a inner join t22 h\n" +
                            "on g.a=h.a inner join t22 i\n" +
                            "on h.a=i.a inner join t22 j\n" +
                            "on i.a=j.a inner join t22 k\n" +
                            "on j.a=k.a inner join t22 l\n" +
                            "on k.a=l.a\n" +
                            "and\n" +
                            "l.b >= (select a from t33)\n" +
                            "where a.a between 1 and 900\n" +
                            "group by a.b) myTab(a) \n" +
                            ",t11\n" +
                            "where t11.a between 1 and 90 and t11.b = myTab.a";
            testQueryContains(explainQuery, "NestedLoopJoin", methodWatcher, true);
        }
    }
}
