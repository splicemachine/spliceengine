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

package com.splicemachine.db.impl.sql.execute;

/* can't import due to name overlap:
import java.util.Date;
*/
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
	CurrentDatetime provides execution support for ensuring
	that the current datetime is evaluated only once for a
	statement. The same value is returned for every
	CURRENT_DATE, CURRENT_TIME, and CURRENT_TIMESTAMP in the
	statement.
	<p>
	This is expected to be used by an activation and its
	result set, and so 'forget' must be called whenever you
	want to reuse the CurrentDatetime object for additional
	executions of the statement.

 */
public class CurrentDatetime {

	/**
		Holds the current datetime on the first evaluation of a current function
		in a statement, which contains all available fields.
	 */
	private java.util.Date currentDatetime;
	/**
		Holds the SQL DATE version of the current datetime.
	 */
	private Date currentDate;
	/**
		Holds the SQL TIME version of the current datetime.
	 */
	private Time currentTime;
	/**
		Holds the SQL TIMESTAMP version of the current datetime.
	 */
	private Timestamp currentTimestamp;

	/**
		The constructor is public; note we wait until evaluation to
		put any values into the fields.
	 */
	public CurrentDatetime() {
	}

	// class implementation
	final private void setCurrentDatetime() {
		if (currentDatetime == null)
			currentDatetime = new java.util.Date();
	}

	// class interface

	public Date getCurrentDate() {
		if (currentDate == null) {
			setCurrentDatetime();
			currentDate = new Date(currentDatetime.getTime());
		}
		return currentDate;
	}

	public Time getCurrentTime() {
		if (currentTime == null) {
			setCurrentDatetime();
			currentTime = new Time(currentDatetime.getTime());
		}
		return currentTime;
	}

	public Timestamp getCurrentTimestamp() {
		if (currentTimestamp == null) {
			setCurrentDatetime();
			currentTimestamp = new Timestamp(currentDatetime.getTime());
		}
		return currentTimestamp;
	}

	/**
		This is called prior to each execution of the statement, to
		ensure that it starts over with a new current datetime value.
	 */
	public void forget() {
		currentDatetime = null;
		currentDate = null;
		currentTime = null;
		currentTimestamp = null;
	}

}
