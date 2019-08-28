/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeMap;

/**
 * Created by jyuan on 10/14/14.
 */
public class SpliceCatalogUpgradeScripts{
    protected static final Logger LOG=Logger.getLogger(SpliceCatalogUpgradeScripts.class);

    SpliceDataDictionary sdd;
    Splice_DD_Version catalogVersion;
    TransactionController tc;
    TreeMap<Splice_DD_Version, UpgradeScript> scripts;
    Comparator<Splice_DD_Version> ddComparator;
    
    public SpliceCatalogUpgradeScripts(SpliceDataDictionary sdd,Splice_DD_Version catalogVersion,TransactionController tc){
        this.sdd=sdd;
        this.catalogVersion=catalogVersion;
        this.tc=tc;

        ddComparator=new Comparator<Splice_DD_Version>(){
            @Override
            public int compare(Splice_DD_Version version1,Splice_DD_Version version2){
                long v1=version1.toLong();
                long v2=version2.toLong();

                if(v1<v2)
                    return -1;
                else if(v1==v2)
                    return 0;
                else
                    return 1;
            }
        };
        scripts=new TreeMap<>(ddComparator);
        scripts.put(new Splice_DD_Version(sdd,1,0,0),new UpgradeScriptForFuji(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,1,1,1),new LassenUpgradeScript(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,6,0),new UpgradeScriptFor260(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,1), new UpgradeScriptForModifySchemaPermissionAndDefaultRole(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1812), new UpgradeScriptToCleanSysRoutinePerms(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1817), new UpgradeScriptForSysTokens(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1842), new UpgradeScriptForDroppedConglomerates(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1849), new UpgradeScriptToRemoveFKDependencyOnPrivileges(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1851), new UpgradeScriptToAddUseExtrapolationInSysColumns(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1901), new UpgradeScriptToRemoveUnusedBackupTables(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1909), new UpgradeScriptForReplication(sdd, tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1917), new UpgradeScriptForMultiTenancy(sdd,tc));
        scripts.put(new Splice_DD_Version(sdd,2,8,0, 1924), new UpgradeScriptToAddPermissionViewsForMultiTenancy(sdd,tc));
        // Two system procedures are moved, so we need to run base script to update all system procedures
        scripts.put(new Splice_DD_Version(sdd,3,1,0, 1928), new UpgradeScriptBase(sdd,tc));
    }

    public void run() throws StandardException{

        // Set the current version to upgrade from.
        // This flag should only be true for the master server.
        Splice_DD_Version currentVersion=catalogVersion;
        SConfiguration configuration= SIDriver.driver().getConfiguration();
        if(configuration.upgradeForced()) {
            currentVersion=new Splice_DD_Version(null,configuration.getUpgradeForcedFrom());
        }

        NavigableSet<Splice_DD_Version> keys=scripts.navigableKeySet();
        for(Splice_DD_Version version : keys){
            if(currentVersion!=null){
                if(ddComparator.compare(version,currentVersion)<0){
                    continue;
                }
            }
            UpgradeScript script=scripts.get(version);
            script.run();
        }
    }
}
