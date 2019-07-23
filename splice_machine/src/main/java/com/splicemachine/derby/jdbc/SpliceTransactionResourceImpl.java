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
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Optional;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public final class SpliceTransactionResourceImpl implements AutoCloseable{
    private static final Logger LOG=Logger.getLogger(SpliceTransactionResourceImpl.class);
    public static String CONNECTION_STRING = "jdbc:splice:"+ SQLConfiguration.SPLICE_DB+";create=true";
    protected ContextManager cm;
    protected ContextService csf;
    protected String username;
    private String dbname;
    private String drdaID;
    private String rdbIntTkn;
    protected SpliceDatabase database;
    protected LanguageConnectionContext lcc;
    protected String ipAddress;

    public SpliceTransactionResourceImpl() throws SQLException{
        this(CONNECTION_STRING, new Properties());
    }

    public SpliceTransactionResourceImpl(String url,Properties info) throws SQLException{
        SpliceLogUtils.debug(LOG,"instance with url %s and properties %s",url,info);
        csf=ContextService.getFactory(); // Singleton - Not Needed
        dbname=InternalDriver.getDatabaseName(url,info); // Singleton - Not Needed
        username=IdUtil.getUserNameFromURLProps(info); // Static
        drdaID=info.getProperty(Attribute.DRDAID_ATTR,null); // Static
        rdbIntTkn = info.getProperty(Attribute.RDBINTTKN_ATTR, null);
        ipAddress = info.getProperty(Property.IP_ADDRESS,null);

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

    public boolean marshallTransaction(TxnView txn) throws StandardException, SQLException {
        return this.marshallTransaction(txn, null);
    }

    public boolean marshallTransaction(TxnView txn, ManagedCache<String, Optional<String>> propertyCache) throws StandardException, SQLException{
        boolean updated = false;
        try {
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "marshallTransaction with transactionID %s", txn);
            }

            cm = csf.newContextManager();
            csf.setCurrentContextManager(cm);
            updated = true;

            ArrayList<String> grouplist = new ArrayList<>();
            grouplist.add(username);
            if (propertyCache != null) {
                database.getDataDictionary().getDataDictionaryCache().setPropertyCache(propertyCache);
            }
            lcc=database.generateLanguageConnectionContext(txn, cm, username,grouplist,drdaID, dbname, rdbIntTkn, CompilerContext.DataSetProcessorType.DEFAULT_CONTROL,false, -1, ipAddress);

            return true;
        } catch (Throwable t) {
            LOG.error("Exception during marshallTransaction", t);
            if (updated)
                close();
            throw t;
        }
    }


    public void close(){
        csf.resetCurrentContextManager(cm);
        csf.removeContext(cm);
    }

    public LanguageConnectionContext getLcc(){
        return lcc;
    }

}

