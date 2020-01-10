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

package com.splicemachine.dbTesting.system.nstest.tester;

import java.sql.Connection;

import com.splicemachine.dbTesting.system.nstest.NsTest;

/**
 * Tester2 - Threads that frequently opens and closed based on a random choice of number
 */
public class Tester2 extends TesterObject {

	//*******************************************************************************
	//
	// Constructor. Get's the name of the thread running this for use in messages
	//
	//*******************************************************************************
	public Tester2(String name) {
		super(name);
	}


	//**************************************************************************************
	//
	// This starts the acutal test operations.  Overrides the startTesting() of parent.
	// Tester2 profile -
	//     The connection is frequently opened and closed based on
	//     a random choice between 1 and MAX_OPERATIONS_PER_CONN number of
	//     transaction batches committed by this client type.  This client will
	//     do Insert/Update/Delete and simple Select queries over a
	//     small to medium set of data determined randomly over MAX_LOW_STRESS_ROWS rows.
	//
	//***************************************************************************************
	public void startTesting() {

		//The following loop will be done nstest.MAX_ITERATIONS times after which we exit the thread
		// Note that the connection is frequently opened & closed.  Autocommit is left on, so
		// per connection, we make MAX_OPERATIONS_PER_CONN number of transaction batches
		// Each transaction batch works over MAX_LOW_STRESS_ROWS number of rows, with each row working
		// as one transaction (since autocommit is on)
		for (int i = 0; i < NsTest.MAX_ITERATIONS; i++) {

			//the connection will now open. It closes at the end of the loop
			connex = getConnection();
			if (connex == null) {
				System.out.println("FAIL: " + getThread_id()
						+ " could not get database connection");
				return; //quit
			}

			//set autocommit to false to keep transaction control in your hand
			//Too many deadlocks amd locking issues if this is not commented out
			try {
				connex.setAutoCommit(false);
			} catch (Exception e) {
				System.out.println("FAIL: " + getThread_id()
						+ "'s setAutoCommit() failed:");
				printException("setting AutoCommit in Tester2", e);
			}

			//also set isolation level to Connection.TRANSACTION_READ_UNCOMMITTED to reduce number of
			// deadlocks
			setIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED);

			//Now do MAX_OPERATIONS_PER_CONN transaction batches within this connection
			for (int numOp = 1; numOp < NsTest.MAX_OPERATIONS_PER_CONN; numOp++) {

				//Now loop through nstest.MAX_LOW_STRESS_ROWS number of times/rows
				// Here, we do randomly do either insert/update/delete operations or one select
				int rnum = (int) (Math.random() * 100) % 4; //returns 0, 1, 2, 3

				switch (rnum) {
				case 0: //do a select operation
					try {
						int numSelected = doSelectOperation(NsTest.MAX_LOW_STRESS_ROWS);
						System.out.println(getThread_id() + " selected "
								+ numSelected + " rows");
					} catch (Exception e) {
						System.out.println("doSelect in thread " + getThread_id()
								+ " threw ");
						printException("doSelectOperation() in Tester2", e);
					}
					break;

				case 1: //do Insert/Update/Delete operations
				case 2: //do Insert/Update/Delete operations
				case 3: //do Insert/Update/Delete operations
					for (int j = 0; j < NsTest.MAX_LOW_STRESS_ROWS; j++) {
						doIUDOperation();
					}
					break;
				}

				//Letting this be even though autocommit is on so that if later on if we decide to turn
				// autocommit off, this automatically takes effect.
				//commit
				try {
					connex.commit();
				} catch (Exception e) {
					System.out.println("FAIL: " + getThread_id()
							+ "'s commit() failed:");
					printException("committing Xn in Tester2", e);
				}
			}//end of for(int numOp=1...)

			//close the connection for the next iteration
			closeConnection();

		}//end of for (int i=0;...)

		System.out.println("Thread " + getThread_id() + " is now terminating");

	}//end of startTesting()

}
