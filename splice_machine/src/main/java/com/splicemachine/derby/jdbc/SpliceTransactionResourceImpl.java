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

package com.splicemachine.derby.jdbc;

import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.SparkExecutionType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Optional;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Used to create a marshall transaction given a connection string and a transaction view.
 * This class it NOT thread safe
 */
public final class SpliceTransactionResourceImpl implements AutoCloseable{
    private static final Logger LOG=Logger.getLogger(SpliceTransactionResourceImpl.class);
    public static final String CONNECTION_STRING = "jdbc:splice:"+ SQLConfiguration.SPLICE_DB+";create=true";
    protected ContextManager cm;
    protected ContextService csf;
    protected String username;
    private String dbname;
    private String drdaID;
    private String rdbIntTkn;
    protected SpliceDatabase database;
    protected LanguageConnectionContext lcc;
    protected String ipAddress;
    private boolean prepared = false;

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

    public void marshallTransaction(TxnView txn) throws StandardException, SQLException {
        this.marshallTransaction(txn, null, null, null, null, null);
    }

    public void marshallTransaction(TxnView txn, ManagedCache<String, Optional<String>> propertyCache,
                                    ManagedCache<UUID, SPSDescriptor>  storedPreparedStatementCache,
                                    List<String> defaultRoles,
                                    SchemaDescriptor initialDefaultSchemaDescriptor,
                                    ManagedCache<Long, Conglomerate> conglomerateCache) throws StandardException, SQLException {
        this.marshallTransaction(txn, propertyCache, storedPreparedStatementCache, defaultRoles, initialDefaultSchemaDescriptor, conglomerateCache, null, null, null);
    }

    public void marshallTransaction(TxnView txn, ManagedCache<String, Optional<String>> propertyCache,
                                    ManagedCache<UUID, SPSDescriptor>  storedPreparedStatementCache,
                                    List<String> defaultRoles,
                                    SchemaDescriptor initialDefaultSchemaDescriptor,
                                    ManagedCache<Long,Conglomerate> conglomerateCache,
                                    TransactionController reuseTC,
                                    String localUserName, Integer sessionNumber) throws StandardException, SQLException{
        if (prepared) {
            throw new IllegalStateException("Cannot create a new marshall Transaction as the last one wasn't closed");
        }
        try {
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "marshallTransaction with transactionID %s", txn);
            }

            cm = csf.newContextManager();
            csf.setCurrentContextManager(cm);
            prepared = true;

            String userName = localUserName != null ? localUserName : username;
            ArrayList<String> grouplist = new ArrayList<>();
            grouplist.add(userName);
            if (propertyCache != null) {
                database.getDataDictionary().getDataDictionaryCache().setPropertyCache(propertyCache);
            }
//            if (schemaCache != null) {
//                database.getDataDictionary().getDataDictionaryCache().
//                    setSchemaCache(schemaCache);
//            }  // msirek-temp
//            if (conglomerateCache != null) {
//                database.getDataDictionary().getDataDictionaryCache().
//                    setConglomerateCache(conglomerateCache);
//            }  // msirek-temp

            lcc=database.generateLanguageConnectionContext(
                    txn, cm, userName,grouplist,drdaID, dbname, rdbIntTkn,
                    DataSetProcessorType.DEFAULT_OLTP, SparkExecutionType.UNSPECIFIED,
                    false, -1,
                    ipAddress,
                    storedPreparedStatementCache,
                    defaultRoles,
                    initialDefaultSchemaDescriptor,
                    conglomerateCache,
                    reuseTC);

        } catch (Throwable t) {
            LOG.error("Exception during marshallTransaction", t);
            if (prepared)
                close();
            throw t;
        }
    }


    public void close(){
        if (prepared) {
            csf.resetCurrentContextManager(cm);
            csf.removeContextManager(cm);
            prepared = false;
        }
    }

    public LanguageConnectionContext getLcc(){
        return lcc;
    }

}

