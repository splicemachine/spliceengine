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

package com.splicemachine.compactions;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.SpliceCompactionUtils;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.PurgeConfig;
import com.splicemachine.si.impl.server.PurgeConfigBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 */
public class HPurgeConfigFactory implements PurgeConfigFactory {
    private static final Logger LOG = Logger.getLogger(HPurgeConfigFactory.class);

    public static boolean engineStarted() {
        return SIDriver.driver() != null && SIDriver.driver().isEngineStarted();
    }

    public static boolean isRestoreMode() {
        return SIDriver.driver() != null && SIDriver.driver().lifecycleManager().isRestoreMode();
    }

    public static long getTxnLowWatermark(String tableName, boolean tableNeedsSI, boolean regionIsClosing) {
        long txnLowWatermark = SIConstants.OLDEST_TIME_TRAVEL_TX;
        if(engineStarted() && tableNeedsSI && !isRestoreMode() && !regionIsClosing) {
            try {
                txnLowWatermark = SpliceCompactionUtils.getTxnLowWatermark(tableName);
            } catch (Exception e) {
                LOG.warn("Could not extract compaction information, we will not purge." +
                        " Exception: ", e);
                assert false;
            }
        }
        return txnLowWatermark;
    }

    public static PurgeConfig compactionConfigHelper(boolean isMajor, long transactionLowWatermark, String tableName) throws IOException {
        SConfiguration config = HConfiguration.getConfiguration();
        PurgeConfigBuilder purgeConfig = new PurgeConfigBuilder();
        if (engineStarted() && SpliceCompactionUtils.forcePurgeDeletes(tableName) && isMajor) {
            purgeConfig.forcePurgeDeletes();
        } else if (config.getOlapCompactionAutomaticallyPurgeDeletedRows()) {
            purgeConfig.purgeDeletesDuringCompaction(isMajor);
        } else {
            purgeConfig.noPurgeDeletes();
        }
        purgeConfig.purgeUpdates(config.getOlapCompactionAutomaticallyPurgeOldUpdates());
        purgeConfig.transactionLowWatermark(transactionLowWatermark);
        return purgeConfig.build();
    }

    @Override
    public PurgeConfig compactionConfig(String tableName, boolean isMajor) throws IOException {
        return compactionConfigHelper(isMajor, SpliceCompactionUtils.getTxnLowWatermark(tableName), tableName);
    }

    @Override
    public PurgeConfig flushConfig(String tableName, boolean tableNeedsSI, boolean regionIsClosing) {
        SIDriver driver=SIDriver.driver();
        PurgeConfigBuilder purgeConfig = new PurgeConfigBuilder();
        SConfiguration conf = driver.getConfiguration();
        if (conf.getOlapCompactionAutomaticallyPurgeDeletedRows()) {
            purgeConfig.purgeDeletesDuringFlush();
        } else {
            purgeConfig.noPurgeDeletes();
        }
        purgeConfig.transactionLowWatermark(getTxnLowWatermark(tableName, tableNeedsSI, regionIsClosing));
        purgeConfig.purgeUpdates(conf.getOlapCompactionAutomaticallyPurgeOldUpdates());
        return purgeConfig.build();
    }

    @Override
    public PurgeConfig memStoreCompactionConfig(String tableName, boolean tableNeedsSI, boolean regionIsClosing) {
        return flushConfig(tableName, tableNeedsSI, regionIsClosing);
    }
}
