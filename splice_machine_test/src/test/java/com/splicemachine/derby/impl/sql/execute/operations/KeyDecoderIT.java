package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Iterator;

import com.google.common.collect.TreeMultiset;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
/**
 * Created by jyuan on 4/3/14.
 */

public class KeyDecoderIT extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(KeyDecoderIT.class);
    private static final String SCHEMA_NAME = KeyDecoderIT.class.getSimpleName().toUpperCase();
    private static final String TABLE1 = "APOLLO_MV_MINUTE";
    private static final String TABLE2 = "T";
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final String  CREATE =
            "(" +
                "row_id bigint NOT NULL, " +
                "ad_id bigint NOT NULL, " +
                "campaign_id bigint NOT NULL, " +
                "day timestamp NOT NULL, " +
                "placement_id bigint NOT NULL, " +
                "publisher_id bigint NOT NULL, " +
                "tactic_id bigint NOT NULL, " +
                "line_item_id bigint NOT NULL, " +
                "rfi_clicks double NOT NULL, " +
                "pub_clicks double NOT NULL, " +
                "adv_clicks double NOT NULL, " +
                "rfi_client_views double NOT NULL, " +
                "pub_client_views double NOT NULL, " +
                "adv_client_views double NOT NULL, " +
                "rfi_server_views double NOT NULL, " +
                "pub_server_views double NOT NULL, " +
                "adv_server_views double NOT NULL, " +
                "cost double NOT NULL, " +
                "rfi_revenue double NOT NULL, " +
                "pub_revenue double NOT NULL, " +
                "adv_revenue double NOT NULL, " +
                "rfi_value double NOT NULL, " +
                "pub_value double NOT NULL, " +
                "adv_value double NOT NULL, " +
                "rfi_conversions double NOT NULL, " +
                "adv_conversions double NOT NULL, " +
                "rfi_thisday_conversions double NOT NULL, " +
                "adv_thisday_conversions double NOT NULL, " +
                "refresh_time varchar(30) NOT NULL, " +
                "tier smallint NOT NULL DEFAULT 3, " +
                "flight_id bigint NOT NULL, " +
                "pub_clicks_source smallint DEFAULT NULL, " +
                "adv_clicks_source smallint DEFAULT NULL, " +
                "pub_client_views_source smallint DEFAULT NULL, " +
                "adv_client_views_source smallint DEFAULT NULL, " +
                "adv_server_views_source smallint DEFAULT NULL, " +
                "pub_server_views_source smallint DEFAULT NULL, " +
                "adv_conversions_source smallint DEFAULT NULL, " +
                "first_touches double NOT NULL DEFAULT 0, " +
                "multi_touches double NOT NULL DEFAULT 0, " +
                "data_cost double DEFAULT NULL, " +
                "media_cost double DEFAULT NULL, " +
                "rfi_client_revenue double NOT NULL, " +
                "adv_client_revenue double NOT NULL, " +
                "media_type smallint NOT NULL DEFAULT 1, " +
                "rfi_thisday_value double NOT NULL DEFAULT 0, " +
                "adv_thisday_value double NOT NULL DEFAULT 0, " +
                "rfi_thisday_client_revenue double NOT NULL, " +
                "adv_thisday_client_revenue double NOT NULL, " +
                "adv_thisday_conversion_source smallint DEFAULT NULL, " +
                "advertiser_id bigint DEFAULT NULL, " +
                "rest_count bigint DEFAULT NULL, " +
                "rfi_conversions_click_through double NOT NULL DEFAULT 0, " +
                "adv_conversions_click_through double NOT NULL DEFAULT 0, " +
                "rfi_thisday_conversions_click_through double NOT NULL DEFAULT 0, " +
                "adv_thisday_conversions_click_through double NOT NULL DEFAULT 0, " +
                "day_campaign_timezone varchar(30) NOT NULL DEFAULT '0000-00-00 00:00:00', " +
                "time_zone varchar(255) NOT NULL DEFAULT 'America/New_York', " +
                "sub_network_id int NOT NULL DEFAULT 1, " +
                "campaign_currency_type char(3) NOT NULL DEFAULT 'USD', " +
                "exchange_rate decimal(13,7) NOT NULL DEFAULT 1.0000000, " +
                "cost_to_advertiser double NOT NULL DEFAULT 0, " +
                "PRIMARY KEY (day, ad_id, row_id) " +
                ")";

    private static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1, SCHEMA_NAME, CREATE);
    private static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2, SCHEMA_NAME, "(i int)");
    private static TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final String dataFile = SpliceUnitTest.getResourceDirectory()+ "x.txt";
    private static SpliceWatcher methodWatcher = new SpliceWatcher();
    private static TreeMultiset<Timestamp> DAYS = TreeMultiset.create();
    private static TreeMultiset<Long> AD_IDS = TreeMultiset.create();
    private static TreeMultiset<Long> ROW_IDS = TreeMultiset.create();
    private static int VAL;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(temporaryFolder)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s', '|',null,null,null,null,0,%s)",
                                                                                     SCHEMA_NAME, TABLE1, dataFile, temporaryFolder.newFolder().getCanonicalPath()));
                        ps.execute();
                        ps.close();
                        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",SCHEMA_NAME,TABLE1));
                        while (rs.next()) {
                            Timestamp day = rs.getTimestamp(4);
                            DAYS.add(day);

                            Long ad_id = new Long(rs.getLong(2));
                            AD_IDS.add(ad_id);

                            Long row_id = new Long(rs.getLong(1));
                            ROW_IDS.add(row_id);
                        }
                        Assert.assertEquals(DAYS.size(), 10);
                        Assert.assertEquals(AD_IDS.size(), 10);
                        Assert.assertEquals(ROW_IDS.size(), 10);
                        rs.close();

                        ps = methodWatcher.prepareStatement(format("insert into %s.%s values 1", SCHEMA_NAME, TABLE2));
                        ps.execute();
                        ps.close();
                        rs = methodWatcher.executeQuery(format("select * from %s.%s", SCHEMA_NAME, TABLE2));
                        while(rs.next()) {
                            VAL = rs.getInt(1);
                        }
                        rs.close();
                    }
                    catch (Exception e) {
                        LOG.error("Error importing data", e);
                    }
                }
            });

    @Test
    public void testSelectDays() throws Exception {
        TreeMultiset<Timestamp> days = TreeMultiset.create();
        ResultSet rs = methodWatcher.executeQuery(format("select day from %s.%s",SCHEMA_NAME,TABLE1));
        while (rs.next()) {
            Timestamp day = rs.getTimestamp(1);
            days.add(day);
        }
        Assert.assertEquals(days.size(), DAYS.size());
        Iterator<Timestamp> IT = DAYS.iterator();
        Iterator<Timestamp> it = days.iterator();
        while(IT.hasNext() && it.hasNext()) {
            Assert.assertEquals(IT.next().compareTo(it.next()), 0);
        }
    }

    @Test
    public void testSelectAdIds() throws Exception {
        TreeMultiset<Long> ad_ids = TreeMultiset.create();
        ResultSet rs = methodWatcher.executeQuery(format("select ad_id from %s.%s",SCHEMA_NAME,TABLE1));
        while (rs.next()) {
            Long ad_id = new Long(rs.getLong(1));
            ad_ids.add(ad_id);
        }
        Assert.assertEquals(ad_ids.size(), AD_IDS.size());
        Iterator<Long> IT = AD_IDS.iterator();
        Iterator<Long> it = ad_ids.iterator();
        while(IT.hasNext() && it.hasNext()) {
            Assert.assertEquals(IT.next().compareTo(it.next()), 0);
        }
    }

    @Test
    public void testSelectRowIds() throws Exception {
        TreeMultiset<Long> row_ids = TreeMultiset.create();
        ResultSet rs = methodWatcher.executeQuery(format("select row_id from %s.%s",SCHEMA_NAME,TABLE1));
        while (rs.next()) {
            Long row_id = new Long(rs.getLong(1));
            row_ids.add(row_id);
        }
        Assert.assertEquals(row_ids.size(), AD_IDS.size());
        Iterator<Long> IT = ROW_IDS.iterator();
        Iterator<Long> it = row_ids.iterator();
        while(IT.hasNext() && it.hasNext()) {
            Assert.assertEquals(IT.next().compareTo(it.next()), 0);
        }
    }

    @Test
    public void testSelectDescIndexColumn() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("create index ti on %s.%s(i desc)", SCHEMA_NAME, TABLE2));
        ps.execute();
        ps.close();
        int val;

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", SCHEMA_NAME, TABLE2));
        while(rs.next()) {
            val = rs.getInt(1);
            Assert.assertEquals(val, VAL);
        }
        rs.close();
    }
}
