/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.catalog.upgrade;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeMap;

import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.derby.impl.sql.catalog.Splice_DD_Version;

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
        scripts.put(new Splice_DD_Version(sdd,2,0,1,1723),new ConglomeratesUpgradeScript(sdd, tc));
    }

    public void run() throws StandardException{

        if(LOG.isInfoEnabled()) LOG.info("Creating/updating system procedures");
        sdd.createOrUpdateAllSystemProcedures(tc);
        if(LOG.isInfoEnabled()) LOG.info("Updating system prepared statements");
        sdd.updateMetadataSPSes(tc);

        // Set the current version to upgrade from.
        // This flag should only be true for the master server.
        Splice_DD_Version currentVersion=catalogVersion;
        SConfiguration configuration= SIDriver.driver().getConfiguration();
        if(configuration.upgradeForced()) {
//        if (SpliceConstants.upgradeForced) {
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
