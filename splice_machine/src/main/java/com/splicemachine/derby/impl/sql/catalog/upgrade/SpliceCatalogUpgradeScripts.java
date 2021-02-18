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

package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.derby.impl.sql.catalog.Splice_DD_Version;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;
import java.util.*;

/**
 * Created by jyuan on 10/14/14.
 */
public class SpliceCatalogUpgradeScripts{
    protected static final Logger LOG=Logger.getLogger(SpliceCatalogUpgradeScripts.class);

    SpliceDataDictionary sdd;
    TransactionController tc;

    List<VersionAndUpgrade> scripts;

    public static class VersionAndUpgrade {
        public Splice_DD_Version version;
        public UpgradeScript script;

        public VersionAndUpgrade(Splice_DD_Version version,UpgradeScript script) {
            this.version = version;
            this.script = script;
        }
    }

    public void addUpgradeScript(Splice_DD_Version version, int sprint, UpgradeScript script)
    {
        scripts.add(new VersionAndUpgrade(new Splice_DD_Version(sdd, version.getMajorVersionNumber(),
                version.getMinorVersionNumber(), version.getPatchVersionNumber(), sprint), script));
    }

    // Upgrade scripts should be mostly the same on master/3.0/3.1.
    // However the base versions (i.e. major.minor.patch without sprint) are different.
    // e.g. UpgradeScriptToAddSysNaturalNumbersTable is for sprint 1985, which is [baseVersion4, 1985].
    // that means on master = 3.2.0.1985, branch-3.0 = 3.1.0.1985, branch-3.0 = 3.0.1.1985.

    // following table gives an overview over different base versions in the branches
    //                 master | branch-3.0 | branch-3.1 | remarks
    // baseVersion1 =  2.8.0  |   2.8.0    |   2.8.0    |
    // baseVersion2 =  3.1.0  |   3.1.0    |   3.0.0    | 2.8 -> 3.x fork
    // baseVersion3 =  3.1.0  |   3.1.0    |   3.0.1    | hickup on branch 3.0
    // baseVersion4 =  3.2.0  |   3.1.0    |   3.0.1    | 3.2 fork
    // ...
    // baseVersionX = ...                               | add here new versions
    //
    // also check SpliceCatalogUpgradeScriptsTest for some unit tests for this.

    static final public Splice_DD_Version baseVersion1 = new Splice_DD_Version(null, 2, 8, 0);
    static final public Splice_DD_Version baseVersion2 = new Splice_DD_Version(null, 3, 1, 0);
    static final public Splice_DD_Version baseVersion3 = new Splice_DD_Version(null, 3, 1, 0);
    static final public Splice_DD_Version baseVersion4 = new Splice_DD_Version(null, 3, 2, 0);

    public SpliceCatalogUpgradeScripts(SpliceDataDictionary sdd, TransactionController tc){
        this.sdd=sdd;
        this.tc=tc;

        scripts = new ArrayList<>();
        // DB-11296: UpgradeConglomerateTable has to be executed first, because it adds a system table
        // CONGLOMERATE_SI_TABLE_NAME that is from then on needed to create tables, e.g.
        // in UpgradeScriptToAddSysNaturalNumbersTable. If UpgradeConglomerateTable is at the end,
        // these upgrades would fail
        addUpgradeScript(baseVersion4, 1996, new UpgradeConglomerateTable(sdd, tc));

        addUpgradeScript(baseVersion1, 1901, new UpgradeScriptToRemoveUnusedBackupTables(sdd,tc));
        addUpgradeScript(baseVersion1, 1909, new UpgradeScriptForReplication(sdd, tc));
        addUpgradeScript(baseVersion1, 1917, new UpgradeScriptForMultiTenancy(sdd,tc));
        addUpgradeScript(baseVersion1, 1924, new UpgradeScriptToAddPermissionViewsForMultiTenancy(sdd,tc));

        addUpgradeScript(baseVersion2, 1933, new UpgradeScriptToUpdateViewForSYSCONGLOMERATEINSCHEMAS(sdd,tc));
        addUpgradeScript(baseVersion2, 1938, new UpgradeScriptForTriggerWhenClause(sdd,tc));
        addUpgradeScript(baseVersion2, 1940, new UpgradeScriptForReplicationSystemTables(sdd,tc));
        addUpgradeScript(baseVersion2, 1941, new UpgradeScriptForTableColumnViewInSYSIBM(sdd,tc));

        addUpgradeScript(baseVersion2, 1948, new UpgradeScriptForAddDefaultToColumnViewInSYSIBM(sdd,tc));
        addUpgradeScript(baseVersion2, 1953, new UpgradeScriptForRemoveUnusedIndexInSYSFILESTable(sdd,tc));
        addUpgradeScript(baseVersion2, 1959, new UpgradeScriptForTriggerMultipleStatements(sdd,tc));
        addUpgradeScript(baseVersion2, 1962, new UpgradeScriptForAddDefaultToColumnViewInSYSVW(sdd,tc));

        addUpgradeScript(baseVersion2, 1964, new UpgradeScriptForAliasToTableView(sdd,tc));
        addUpgradeScript(baseVersion2, 1970, new UpgradeScriptForAddTablesAndViewsInSYSIBMADM(sdd,tc));
        addUpgradeScript(baseVersion2, 1971, new UpgradeScriptToAddCatalogVersion(sdd,tc));
        addUpgradeScript(baseVersion2, 1974, new UpgradeScriptToAddMinRetentionPeriodColumnToSYSTABLES(sdd, tc));

        addUpgradeScript(baseVersion2, 1977, new UpgradeScriptToAddSysKeyColUseViewInSYSIBM(sdd, tc));
        addUpgradeScript(baseVersion3, 1979, new UpgradeScriptToSetJavaClassNameColumnInSYSALIASES(sdd, tc));

        addUpgradeScript(baseVersion4, 1983, new UpgradeScriptToAddBaseTableSchemaColumnsToSysTablesInSYSIBM(sdd,tc));
        addUpgradeScript(baseVersion4, 1985, new UpgradeScriptToAddSysNaturalNumbersTable(sdd, tc));
        addUpgradeScript(baseVersion4, 1989, new UpgradeScriptToAddIndexColUseViewInSYSCAT(sdd, tc));
        addUpgradeScript(baseVersion4, 1992, new UpgradeScriptForTablePriorities(sdd, tc));
        addUpgradeScript(baseVersion4, 1993, new UpgradeScriptToAddSysIndexesViewInSYSIBMAndUpdateIndexColUseViewInSYSCAT(sdd, tc));
        addUpgradeScript(baseVersion4, 1996, new UpgradeScriptToAddReferencesViewInSYSCAT(sdd, tc));
        addUpgradeScript(baseVersion4, 2001, new UpgradeScriptToAddColumnsViewInSYSCAT(sdd, tc));

        // remember to add your script to SpliceCatalogUpgradeScriptsTest too, otherwise test fails
    }

    public static List<VersionAndUpgrade> getScriptsToUpgrade(List<VersionAndUpgrade> scripts,
                                                              Splice_DD_Version currentVersion)
    {
        List<VersionAndUpgrade> upgradeNeeded = new ArrayList<>();
        for(VersionAndUpgrade el : scripts){
            if(currentVersion!=null){
                if(Splice_DD_Version.compare(el.version,currentVersion)<=0){
                    continue;
                }
            }
            upgradeNeeded.add(el);
        }
        return upgradeNeeded;
    }

    public List<VersionAndUpgrade> getScripts() {
        return scripts;
    }

    public static void runAllScripts(List<VersionAndUpgrade> upgradeNeeded) throws StandardException {
        if( upgradeNeeded.size() == 0 ) {
            LOG.info("No upgrade needed.");
            return;
        }
        LOG.info("Running " + upgradeNeeded.size() + " upgrade scripts:");
        for( VersionAndUpgrade el : upgradeNeeded ) {
            LOG.info("Running upgrade script " + el.version + ": " + el.script.getClass().getName());
            el.script.run();
        }
        LOG.info("upgrade done.");
    }

    public void runUpgrades(Splice_DD_Version catalogVersion) throws StandardException{
        LOG.info("Catalog is on version " + catalogVersion + ". checking for upgrades...");
        // Set the current version to upgrade from.
        // This flag should only be true for the master server.
        Splice_DD_Version currentVersion=catalogVersion;
        SConfiguration configuration= SIDriver.driver().getConfiguration();
        if(configuration.upgradeForced()) {
            currentVersion=new Splice_DD_Version(null,configuration.getUpgradeForcedFrom());
        }
        runAllScripts(getScriptsToUpgrade(scripts, currentVersion));

        // Always update system procedures and stored statements
        if( sdd != null ) {
            sdd.clearSPSPlans();
            sdd.createOrUpdateAllSystemProcedures(tc);
            sdd.updateMetadataSPSes(tc);
        }
    }
}
