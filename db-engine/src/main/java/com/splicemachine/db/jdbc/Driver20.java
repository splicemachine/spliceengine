/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.jdbc;

import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.Property;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.jdbc.BrokeredConnection;
import com.splicemachine.db.iapi.jdbc.BrokeredConnectionControl;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.io.FormatableProperties;
import com.splicemachine.db.iapi.security.SecurityUtil;

import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet20;
import com.splicemachine.db.impl.jdbc.EmbedStatement;

import java.sql.*;

import java.security.Permission;
import java.security.AccessControlException;

import java.util.Properties;
import java.util.logging.Logger;

/**
	This class extends the local JDBC driver in order to determine at JBMS
	boot-up if the JVM that runs us does support JDBC 2.0. If it is the case
	then we will load the appropriate class(es) that have JDBC 2.0 new public
	methods and sql types.
*/

public abstract class Driver20 extends InternalDriver implements Driver {

	private static final String[] BOOLEAN_CHOICES = {"false", "true"};

	private Class  antiGCDriverManager;

	/*
	**	Methods from ModuleControl
	*/

	public void boot(boolean create, Properties properties) throws StandardException {

		super.boot(create, properties);

		// Register with the driver manager
		AutoloadedDriver.registerDriverModule( this );

		// hold onto the driver manager to avoid its being garbage collected.
		// make sure the class is loaded by using .class
		antiGCDriverManager = java.sql.DriverManager.class;
	}

	public void stop() {

		super.stop();

		AutoloadedDriver.unregisterDriverModule();
	}
  
	public EmbedResultSet
	newEmbedResultSet(EmbedConnection conn, ResultSet results, boolean forMetaData, EmbedStatement statement, boolean isAtomic)
		throws SQLException
	{
		return new EmbedResultSet20(conn, results, forMetaData, statement,
								 isAtomic); 
	}

//	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException{ throw new SQLFeatureNotSupportedException(); }


	public abstract BrokeredConnection newBrokeredConnection(
            BrokeredConnectionControl control) throws SQLException;

    /**
     * <p>The getPropertyInfo method is intended to allow a generic GUI tool to 
     * discover what properties it should prompt a human for in order to get 
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to getPropertyInfo.
     *
     * @param url The URL of the database to connect to.
     * @param info A proposed list of tag/value pairs that will be sent on
     *          connect open.
     * @return An array of DriverPropertyInfo objects describing possible
     *          properties.  This array may be an empty array if no properties
     *          are required.
     * @exception SQLException if a database-access error occurs.
     */
	public  DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {

		// RESOLVE other properties should be added into this method in the future ... 

        if (info != null) {
			if (Boolean.valueOf(info.getProperty(Attribute.SHUTDOWN_ATTR))) {
	
				// no other options possible when shutdown is set to be true
				return new DriverPropertyInfo[0];
			}
		}

		// at this point we have databaseName, 

		String dbname = InternalDriver.getDatabaseName(url, info);

		// convert the ;name=value attributes in the URL into
		// properties.
		FormatableProperties finfo = getAttributes(url, info);
		info = null; // ensure we don't use this reference directly again.
		boolean encryptDB = Boolean.valueOf(finfo.getProperty(Attribute.DATA_ENCRYPTION));
		String encryptpassword = finfo.getProperty(Attribute.BOOT_PASSWORD);

		if (dbname.isEmpty() || (encryptDB && encryptpassword == null)) {

			// with no database name we can have shutdown or a database name

			// In future, if any new attribute info needs to be included in this
			// method, it just has to be added to either string or boolean or secret array
			// depending on whether it accepts string or boolean or secret(ie passwords) value. 

			String[][] connStringAttributes = {
				{Attribute.DBNAME_ATTR, MessageId.CONN_DATABASE_IDENTITY},
				{Attribute.CRYPTO_PROVIDER, MessageId.CONN_CRYPTO_PROVIDER},
				{Attribute.CRYPTO_ALGORITHM, MessageId.CONN_CRYPTO_ALGORITHM},
				{Attribute.CRYPTO_KEY_LENGTH, MessageId.CONN_CRYPTO_KEY_LENGTH},
				{Attribute.CRYPTO_EXTERNAL_KEY, MessageId.CONN_CRYPTO_EXTERNAL_KEY},
				{Attribute.TERRITORY, MessageId.CONN_LOCALE},
				{Attribute.COLLATION, MessageId.CONN_COLLATION},
				{Attribute.USERNAME_ATTR, MessageId.CONN_USERNAME_ATTR},
				{Attribute.LOG_DEVICE, MessageId.CONN_LOG_DEVICE},
				{Attribute.ROLL_FORWARD_RECOVERY_FROM, MessageId.CONN_ROLL_FORWARD_RECOVERY_FROM},
				{Attribute.CREATE_FROM, MessageId.CONN_CREATE_FROM},
				{Attribute.RESTORE_FROM, MessageId.CONN_RESTORE_FROM},
			};

			String[][] connBooleanAttributes = {
				{Attribute.SHUTDOWN_ATTR, MessageId.CONN_SHUT_DOWN_CLOUDSCAPE},
                {Attribute.DEREGISTER_ATTR, MessageId.CONN_DEREGISTER_AUTOLOADEDDRIVER},
				{Attribute.CREATE_ATTR, MessageId.CONN_CREATE_DATABASE},
				{Attribute.DATA_ENCRYPTION, MessageId.CONN_DATA_ENCRYPTION},
				{Attribute.UPGRADE_ATTR, MessageId.CONN_UPGRADE_DATABASE},
				};

			String[][] connStringSecretAttributes = {
				{Attribute.BOOT_PASSWORD, MessageId.CONN_BOOT_PASSWORD},
				{Attribute.PASSWORD_ATTR, MessageId.CONN_PASSWORD_ATTR},
				};

			
			DriverPropertyInfo[] optionsNoDB = new 	DriverPropertyInfo[connStringAttributes.length+
																	  connBooleanAttributes.length+
			                                                          connStringSecretAttributes.length];
			
			int attrIndex = 0;
			for( int i = 0; i < connStringAttributes.length; i++, attrIndex++ )
			{
				optionsNoDB[attrIndex] = new DriverPropertyInfo(connStringAttributes[i][0], 
									  finfo.getProperty(connStringAttributes[i][0]));
				optionsNoDB[attrIndex].description = MessageService.getTextMessage(connStringAttributes[i][1]);
			}

			optionsNoDB[0].choices = Monitor.getMonitor().getServiceList(Property.DATABASE_MODULE);
			// since database name is not stored in FormatableProperties, we
			// assign here explicitly
			optionsNoDB[0].value = dbname;

			for( int i = 0; i < connStringSecretAttributes.length; i++, attrIndex++ )
			{
				optionsNoDB[attrIndex] = new DriverPropertyInfo(connStringSecretAttributes[i][0], 
									  (finfo.getProperty(connStringSecretAttributes[i][0]) == null? "" : "****"));
				optionsNoDB[attrIndex].description = MessageService.getTextMessage(connStringSecretAttributes[i][1]);
			}

			for( int i = 0; i < connBooleanAttributes.length; i++, attrIndex++ )
			{
				optionsNoDB[attrIndex] = new DriverPropertyInfo(connBooleanAttributes[i][0], 
           		    Boolean.valueOf(finfo == null? "" : finfo.getProperty(connBooleanAttributes[i][0])).toString());
				optionsNoDB[attrIndex].description = MessageService.getTextMessage(connBooleanAttributes[i][1]);
				optionsNoDB[attrIndex].choices = BOOLEAN_CHOICES;				
			}

			return optionsNoDB;
		}

		return new DriverPropertyInfo[0];
	}

    /**
     * Checks for System Privileges.
     *
     * @param user The user to be checked for having the permission
     * @param perm The permission to be checked
     * @throws AccessControlException if permissions are missing
     * @throws Exception if the privileges check fails for some other reason
     */
    public void checkSystemPrivileges(String user,
                                      Permission perm)
        throws Exception {
        SecurityUtil.checkUserHasPermission(user, perm);
    }
}
