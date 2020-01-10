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

package com.splicemachine.dbTesting.unitTests.services;

/**

	Key for these objects is an array of objects

	value - Integer or String - implies what object should be used in the cache.
	waitms - time to wait in ms on a set or create (simulates the object being loaded into the cache).
	canFind - true of the object can be found on a set, false if it can't. (simulates a request for a non-existent object)
	raiseException - true if an exception should be raised during set or create identity


*/
public class T_Key  {

	private Object	value;
	private long		waitms;
	private boolean   canFind;
	private boolean   raiseException;

	public static T_Key		simpleInt(int value) {
		return new T_Key(new Integer(value), 0, true, false);
	}
	public static T_Key		dontFindInt(int value) {
		return new T_Key(new Integer(value), 0, false, false);
	}
	public static T_Key		exceptionInt(int value) {
		return new T_Key(new Integer(value), 0, true, true);
	}
	
	/**
		48%/48%/4% chance of Int/String/invalid key
		90%/5%/5% chance of can find / can't find / raise exception
	*/
	public static T_Key randomKey() {

		double rand = Math.random();
		T_Key tkey = new T_Key();

		if (rand < 0.48)
			tkey.value = new Integer((int) (100.0 * rand));
		else if (rand < 0.96)
			tkey.value = new Integer((int) (100.0 * rand));
		else
			tkey.value = Boolean.FALSE;

		rand = Math.random();

		if (rand < 0.90)
			tkey.canFind = true;
		else if (rand < 0.95)
			tkey.canFind = false;
		else {
			tkey.canFind = true;
			tkey.raiseException = false;
		}

		rand = Math.random();

		if (rand < 0.30) {
			tkey.waitms = (long) (rand * 1000.0); // Range 0 - 0.3 secs
		}

		return tkey;
	}

	private T_Key() {
	}


	private T_Key(Object value, long waitms, boolean canFind, boolean raiseException) {

		this.value = value;
		this.waitms = waitms;
		this.canFind = canFind;
		this.raiseException = raiseException;
	}

	public Object getValue() {
		return value;
	}

	public long getWait() {
		return waitms;
	}

	public boolean canFind() {
		return canFind;
	}

	public boolean raiseException() {
		return raiseException;
	}

	public boolean equals(Object other) {
		if (other instanceof T_Key) {
			return value.equals(((T_Key) other).value);
		}
		return false;
	}

	public int hashCode() {
		return value.hashCode();
	}

	public String toString() {
		return value + " " + waitms + " " + canFind + " " + raiseException;
	}
}

