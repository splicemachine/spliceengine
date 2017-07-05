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

import java.sql.*;

import java.util.Properties;
import java.util.logging.Logger;

import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.impl.jdbc.Util;


/**
   This is the dummy driver which is registered with the DriverManager and
   which is autoloaded by JDBC4. Loading this class will NOT automatically
   boot the Derby engine, but it will register this class as a valid
   Driver with the DriverManager.
   Instead, the engine boots lazily when you ask for a
   Connection. Alternatively, you can force the engine to boot as follows:

   	 <PRE>
	 Class.forName("com.splicemachine.db.jdbc.EmbeddedDriver").newInstance();

	 // or

     new com.splicemachine.db.jdbc.EmbeddedDriver();

    
	</PRE>
*/
public class AutoloadedDriver implements Driver {
	// This flag is set if the engine is forcibly brought down.
	private	static	boolean	_engineForcedDown = false;
	

    // This is the driver that memorizes the autoloadeddriver (DERBY-2905)
    private static Driver _autoloadedDriver;

	//
	// This is the driver that's specific to the JDBC level we're running at.
	// It's the module which boots the whole Derby engine.
	//
	private	static	Driver	_driverModule;
	
	static
	{
        try {
            //
            // We'd rather load this slightly more capable driver.
            // But if the vm level doesn't support it, then we fall
            // back on the JDBC3 level driver.
            //
            Class.forName( "com.splicemachine.db.jdbc.AutoloadedDriver40" );
        }
        catch (Throwable e)
        {
            registerMe( new AutoloadedDriver() );
        }
	}

	protected static void   registerMe( AutoloadedDriver me )
	{
		try {
            _autoloadedDriver = me;
            DriverManager.registerDriver( _autoloadedDriver );
		}
		catch (SQLException se)
		{
			String	message = MessageService.getTextMessage
				(MessageId.JDBC_DRIVER_REGISTER_ERROR, se.getMessage() );

			throw new IllegalStateException( message );
		}
	}

	/*
	** Methods from java.sql.Driver.
	*/
	/**
		Accept anything that starts with <CODE>jdbc:splice:</CODE>.
		@exception SQLException if a database-access error occurs.
    @see java.sql.Driver
	*/
	public boolean acceptsURL(String url) throws SQLException {

		//
		// We don't want to accidentally boot the engine just because
		// the application is looking for a connection from some other
		// driver.
		//
		return !_engineForcedDown && InternalDriver.embeddedDriverAcceptsURL(url);
	}

   
	/**
		Connect to the URL if possible
		@exception SQLException illegal url or problem with connectiong
    @see java.sql.Driver
  */
	public Connection connect(String url, Properties info)
		throws SQLException
	{
		//
		// This pretty piece of logic compensates for the following behavior
		// of the DriverManager: When asked to get a Connection, the
		// DriverManager cycles through all of its autoloaded drivers, looking
		// for one which will return a Connection. Without this pretty logic,
		// the embedded driver module will be booted by any request for
		// a connection which cannot be satisfied by drivers ahead of us
		// in the list.
		if (!InternalDriver.embeddedDriverAcceptsURL(url)) { return null; }

		return getDriverModule().connect(url, info);
	}

  /**
   * Returns an array of DriverPropertyInfo objects describing possible properties.
    @exception SQLException if a database-access error occurs.
    @see java.sql.Driver
   */
	public  DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
		throws SQLException
	{
		return getDriverModule().getPropertyInfo(url, info);
	}

    /**
     * Returns the driver's major version number. 
     @see java.sql.Driver
     */
	public int getMajorVersion() {
		try {
			return (getDriverModule().getMajorVersion());
		}
		catch (SQLException se) {
			return 0;
		}
	}
    /**
     * Returns the driver's minor version number.
     @see java.sql.Driver
     */
	public int getMinorVersion() {
		try {
			return (getDriverModule().getMinorVersion());
		}
		catch (SQLException se) {
			return 0;
		}
	}

  /**
   * Report whether the Driver is a genuine JDBC COMPLIANT (tm) driver.
     @see java.sql.Driver
   */
	public boolean jdbcCompliant() {
		try {
			return (getDriverModule().jdbcCompliant());
		}
		catch (SQLException se) {
			return false;
		}
	}

//	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException{
		throw new SQLFeatureNotSupportedException();
	}
	///////////////////////////////////////////////////////////////////////
	//
	// Support for booting and shutting down the engine.
	//
	///////////////////////////////////////////////////////////////////////

	/*
	** Retrieve the driver which is specific to our JDBC level.
	** We defer real work to this specific driver.
	*/
	static	Driver getDriverModule() throws SQLException {

		if ( _engineForcedDown && (_autoloadedDriver == null))
		{
			// Driver not registered
            throw Util.generateCsSQLException( SQLState.CORE_JDBC_DRIVER_UNREGISTERED );
		}

		if ( !isBooted() ) { EmbeddedDriver.boot(); }

		return _driverModule;
	}
	
	/**
	** Record which driver module actually booted.
	*  @param driver the driver register to DriverManager is not AutoloadedDriver
	**/
	static	void	registerDriverModule( Driver driver )
	{
		_driverModule = driver;
		_engineForcedDown = false;
		
        try {
            if (_autoloadedDriver == null) {
                //Support JDBC 4 or higher (DERBY-2905)
                _autoloadedDriver = makeAutoloadedDriver();
                DriverManager.registerDriver(_autoloadedDriver);
            }
        } catch (SQLException e) {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT(e);
        }
	}
	
	/**
	** Unregister the driver and the AutoloadedDriver if exists. 
	*  This happens when the engine is forcibly shut down.
	*  
	**/
	static	void	unregisterDriverModule()
	{
		_engineForcedDown = true;
        try {
            // deregister is false if user set deregister=false attribute (DERBY-2905)
            if (InternalDriver.getDeregister() && _autoloadedDriver != null) {
                DriverManager.deregisterDriver(_autoloadedDriver);
                _autoloadedDriver = null;
            } else {
                DriverManager.deregisterDriver(_driverModule);
                //DERBY 5085, need to restore the default value
                InternalDriver.setDeregister(true);
            }
            _driverModule = null;
        } catch (SQLException e) {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT(e);
        }
	}
	

	/*
	** Return true if the engine has been booted.
	*/
	public static	boolean	isBooted()
	{
		return ( _driverModule != null );
	}
	
    /**
     * load slightly more capable driver if possible.
     * But if the vm level doesn't support it, then we fall
     * back on the JDBC3 level driver.
     * @return AutoloadedDriver 
     */
    private static AutoloadedDriver makeAutoloadedDriver() 
    { 
        try { 
            return (AutoloadedDriver) Class.forName( "com.splicemachine.db.jdbc.AutoloadedDriver40" ).newInstance();
        } 
        catch (Throwable ignored) {}

        return new AutoloadedDriver(); 
    } 
}

