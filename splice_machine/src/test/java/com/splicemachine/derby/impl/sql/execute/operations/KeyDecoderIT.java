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

package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Iterator;

import splice.com.google.common.collect.TreeMultiset;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
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
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();
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
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s', '|',null,null,null,null,0,'%s',true,null)",
                                SCHEMA_NAME, TABLE1, dataFile, temporaryFolder.newFolder().getCanonicalPath()));
                        ps.execute();
                        ps.close();
                        ResultSet rs = spliceClassWatcher.executeQuery(format("select * from %s.%s", SCHEMA_NAME, TABLE1));
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

                        ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s values 1", SCHEMA_NAME, TABLE2));
                        ps.execute();
                        ps.close();
                        rs = spliceClassWatcher.executeQuery(format("select * from %s.%s", SCHEMA_NAME, TABLE2));
                        while (rs.next()) {
                            VAL = rs.getInt(1);
                        }
                        rs.close();
                    } catch (Exception e) {
                        LOG.error("Error importing data", e);
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
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
        Iterator<Timestamp> expected = DAYS.iterator();
        Iterator<Timestamp> it = days.iterator();
        while(expected.hasNext() && it.hasNext()) {
            Assert.assertEquals(expected.next().compareTo(it.next()), 0);
        }
        Assert.assertFalse(expected.hasNext());
        Assert.assertFalse(it.hasNext());
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
        Iterator<Long> expected = AD_IDS.iterator();
        Iterator<Long> it = ad_ids.iterator();
        while(expected.hasNext() && it.hasNext()) {
            Assert.assertEquals(expected.next().compareTo(it.next()), 0);
        }
        Assert.assertFalse(expected.hasNext());
        Assert.assertFalse(it.hasNext());
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
        Iterator<Long> expected = ROW_IDS.iterator();
        Iterator<Long> it = row_ids.iterator();
        while(expected.hasNext() && it.hasNext()) {
            Assert.assertEquals(expected.next().compareTo(it.next()), 0);
        }
        Assert.assertFalse(expected.hasNext());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testSelectDescIndexColumn() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("create index ti on %s.%s(i desc)", SCHEMA_NAME, TABLE2));
        ps.execute();
        ps.close();
        int val;

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", SCHEMA_NAME, TABLE2));
        boolean next = rs.next();
        Assert.assertTrue(next);
        val = rs.getInt(1);
        Assert.assertEquals(val, VAL);
        rs.close();
    }
}
