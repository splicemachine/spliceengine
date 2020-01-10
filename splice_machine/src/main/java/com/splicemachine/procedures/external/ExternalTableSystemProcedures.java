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

package com.splicemachine.procedures.external;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.utils.SpliceAdmin;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * Created by jfilali on 11/18/16.
 */
public class ExternalTableSystemProcedures {

    private static Logger LOG = Logger.getLogger(ExternalTableSystemProcedures.class);

    /**
     * This will refresh the schema of th external file. This is useful when some modify the file
     * outside of Splice.
     * It will grab the schema and the table descriptor, then get the file location and will create
     * a job for spark that will force the schema to refresh.
     *
     * @param schema
     * @param table
     * @throws StandardException
     * @throws SQLException
     */

    public static void SYSCS_REFRESH_EXTERNAL_TABLE(String schema, String table) throws StandardException, SQLException {
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        TransactionController tc=lcc.getTransactionExecute();
        try {
            DataDictionary data_dictionary=lcc.getDataDictionary();
            SchemaDescriptor sd=
                    data_dictionary.getSchemaDescriptor(schema,tc,true);
            TableDescriptor td=
                    data_dictionary.getTableDescriptor(table,sd,tc);

            if(td ==null)
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, schema +"." + table);

            if(!td.isExternal())
                throw StandardException.newException(SQLState.NOT_AN_EXTERNAL_TABLE, td.getName());

            String jobGroup = lcc.getSessionUserId() + " <" + tc.getTransactionIdString() +">";

            EngineDriver.driver().getOlapClient().execute(new DistributedRefreshExternalTableSchemaJob(jobGroup, td.getLocation()));

        }catch (Throwable t){
            throw StandardException.plainWrapException(t);

        }
    }

}
