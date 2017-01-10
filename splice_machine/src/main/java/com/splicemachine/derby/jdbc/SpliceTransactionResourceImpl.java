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

package com.splicemachine.derby.jdbc;

import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.sql.SQLException;
import java.util.Properties;

public final class SpliceTransactionResourceImpl implements AutoCloseable{
    private static final Logger LOG=Logger.getLogger(SpliceTransactionResourceImpl.class);
    protected ContextManager cm;
    protected ContextService csf;
    protected String username;
    private String dbname;
    private String drdaID;
    protected SpliceDatabase database;
    protected LanguageConnectionContext lcc;
    private boolean generateLcc=true;
    private ContextManager oldCm;

    public SpliceTransactionResourceImpl() throws SQLException{
        this("jdbc:splice:"+ SQLConfiguration.SPLICE_DB+";create=true", new Properties());
    }

    public SpliceTransactionResourceImpl(String url,Properties info) throws SQLException{
        SpliceLogUtils.debug(LOG,"instance with url %s and properties %s",url,info);
        csf=ContextService.getFactory(); // Singleton - Not Needed
        dbname=InternalDriver.getDatabaseName(url,info); // Singleton - Not Needed
        username=IdUtil.getUserNameFromURLProps(info); // Static
        drdaID=info.getProperty(Attribute.DRDAID_ATTR,null); // Static

        database=(SpliceDatabase)Monitor.findService(Property.DATABASE_MODULE,dbname);
        if(database==null){
            SpliceLogUtils.debug(LOG,"database has not yet been created, creating now");
            try{
                if(!Monitor.startPersistentService(dbname,info)){
                    throw new IllegalArgumentException("Unable to start database!");
                }
                database=(SpliceDatabase)Monitor.findService(Property.DATABASE_MODULE,dbname);
            }catch(StandardException e){
                SpliceLogUtils.error(LOG,e);
                throw PublicAPI.wrapStandardException(e);
            }
        }
    }

    public boolean marshallTransaction(Txn txn) throws StandardException, SQLException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"marshallTransaction with transactionID %s",txn);

        oldCm=csf.getCurrentContextManager();
        cm=csf.newContextManager();
        lcc=database.generateLanguageConnectionContext(txn, cm, username, drdaID, dbname, CompilerContext.DataSetProcessorType.DEFAULT_CONTROL);

        return true;
    }


    public void close(){
        if(generateLcc){
            while(!cm.isEmpty()){
                cm.popContext();
            }
            csf.resetCurrentContextManager(cm);
            csf.removeContext(cm);
            if(oldCm!=null){
                csf.forceRemoveContext(cm);
                oldCm.setActiveThread();
                csf.setCurrentContextManager(oldCm);
            }
        }
    }

    public LanguageConnectionContext getLcc(){
        return lcc;
    }

}

