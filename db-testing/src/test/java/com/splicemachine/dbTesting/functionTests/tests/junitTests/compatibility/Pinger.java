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
/**
 * <p>
 * Ping the server, waiting till it comes up.
 * </p>
 *
 */
package com.splicemachine.dbTesting.functionTests.tests.junitTests.compatibility;

import com.splicemachine.db.drda.NetworkServerControl;

public	class	Pinger
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	public	static	final			long	SLEEP_TIME_MILLIS = 5000L;

	public	static	final			int		SUCCESS_EXIT = 0;
	public	static	final			int		FAILURE_EXIT = 1;
	
	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////
	
	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	public	Pinger() {}

	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINT
	//
	/////////////////////////////////////////////////////////////
	
	public	static	void	main( String[] args )
		throws Exception
	{
		Pinger	me = new Pinger();
		
		me.ping( 5 );
	}	

	/////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////
	
	private	void	println( String text )
	{
		System.err.println( text );
		System.err.flush();
	}

	private	void	exit( int exitStatus )
	{
		Runtime.getRuntime().exit( exitStatus );
	}

	/////////////////////
	//
	//	SERVER MANAGEMENT
	//
	/////////////////////

	/**
	 * <p>
	 * Checks to see that the server is up. If the server doesn't
	 * come up in a reasonable amount of time, brings down the VM.
	 * </p>
	 */
	public	void	ping( int iterations )
		throws Exception
	{
		ping( new NetworkServerControl(), iterations );
	}


	private	void	ping( NetworkServerControl controller, int iterations )
		throws Exception
	{
		Exception	finalException = null;
		
		for ( int i = 0; i < iterations; i++ )
		{
			try {
				controller.ping();

				return;
			}
			catch (Exception e) { finalException = e; }
			
			Thread.sleep( SLEEP_TIME_MILLIS );
		}

		println( "Server did not come up: " + finalException.getMessage() );
		exit( FAILURE_EXIT );
	}


}

