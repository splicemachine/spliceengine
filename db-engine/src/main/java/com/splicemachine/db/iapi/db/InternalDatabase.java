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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.db;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.i18n.LocaleFinder;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.impl.sql.execute.JarUtil;

import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * The com.splicemachine.db.iapi.db.Database
 * interface provides "internal" methods on the database which are
 * not available to JBMS users (com.splicemachine.db.database.Database,
 * which this interface extends, provides all the externally visible
 * methods).
 * <P>
 * At the present moment, this file defines methods which will at
 * some point be moved to to the external database interface.
 *
 * <B> There are a bunch of the unimplemninted interface that used to be in
 * this file.  They have been moved to old_Database.java.  old_Database.java is
 * checked into the codeline but is not built, it is there for reference </B>
 *
 */

public interface InternalDatabase extends com.splicemachine.db.database.Database, LocaleFinder
{
	// this interface gets used on a module, so we name it:
	// Note that doers not point to this class name, but instead to
	// the public API for this class. This ensures that the name
	// written in service.properties is not a obfuscated one.

	/**
     * Sets up a connection to the Database, owned by the given user.
	 *
	 * The JDBC version of getConnection takes a URL. The purpose
	 * of the URL is to tell the driver where the database system is.
	 * By the time we get here, we have found the database system
	 * (that's how we're making this method call), so the URL is not
	 * necessary to establish the connection here. The driver should
	 * remember the URL that was used to establish the connection,
	 * so it can implement the DatabaseMetaData.getURL() method.
	 *
	 * @param user	The UserID of the user getting the connection
	 * @param drdaID	The drda id of the connection (from network server)
	 * @param dbname	The database name
	 *
	 * @return	A new LanguageConnectionContext
	 *
	 * @exception StandardException thrown if unable to create the connection.
	 */
	public LanguageConnectionContext setupConnection(ContextManager cm, String user, List<String> groupuserlist, String drdaID, String dbname,
													 String rdbIntTkn,
													 DataSetProcessorType dataSetProcessorType,
													 boolean skipStats,
													 double defaultSelectivityFactor,
													 String ipAddress,
													 String defaultSchema,
													 Properties sessionProperties
                                                     ) throws StandardException;

	/**
	  Push a DbContext onto the provided context stack. This conext will
	  shut down the database in case of a DatabaseException being
	  cleaned up.
	 */
	void pushDbContext(ContextManager cm);

	/**
		Is the database active (open).
	*/
	boolean isActive();

	/**
	  */
	int	getEngineType();

	/**
	 * This method returns the authentication service handle for the
	 * database.
	 *
	 * NOTE: There is always a Authentication Service per database
	 * and at the system level.
	 *
	 * @return	The authentication service handle for the database
	 * @exception StandardException Derby exception policy
	 */
	AuthenticationService getAuthenticationService()
		throws StandardException;

	/**
	 * Get a Resource Adapter - only used by XA system.  There is one and only
	 * one resource adapter per Derby database.
	 *
	 * @return the resource Adapter for the database, null if no resource
	 * adapter is available for this database. Returned as an Object
	 * so that non-XA aggressive JVMs such as Chai don't get ClassNotFound.
	 * caller must cast result to ResourceAdapter.
	 *
	 */
	Object getResourceAdapter();

	/** Set the Locale that is returned by this LocaleFinder */
	void	setLocale(Locale locale);

    /**
     * Return the DataDictionary for this database, set up at boot time.
     */
	DataDictionary getDataDictionary();

	long addJar(final InputStream is, JarUtil util) throws StandardException;

	void dropJar(JarUtil util) throws StandardException;

	long replaceJar(final InputStream is, JarUtil util) throws StandardException;
}
