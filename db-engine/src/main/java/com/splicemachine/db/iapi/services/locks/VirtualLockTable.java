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

package com.splicemachine.db.iapi.services.locks;

/**
	This class acts as a conduit of information between the lock manager and
	the outside world.  Once a virtual lock table is initialized, it contains
	a snap shot of all the locks currently held in the lock manager.  A VTI can
	then be written to query the content of the lock table.
	<P>
	Each lock held by the lock manager is represented by a Hashtable.  The key
	to each Hashtable entry is a lock attribute that is of interest to the
	outside world, such as transaction id, type, mode, etc.  
 */

public interface VirtualLockTable {

	// flags for Lockable.lockAttributes
	int LATCH = 1;
	int TABLE_AND_ROWLOCK = 2;
    int SHEXLOCK = 4;
	int ALL = ~0;	// turn on all bits

	// This is a list of attributes that is known to the Virtual Lock Table.

	// list of attributes to be supplied by a participating Lockable
	String LOCKTYPE		= "TYPE";	// mandatory
	String LOCKNAME		= "LOCKNAME"; // mandatory
		 // either one of conglomId or containerId mandatory
		 String CONGLOMID	= "CONGLOMID";
	String CONTAINERID	= "CONTAINERID";
	String SEGMENTID	= "SEGMENTID";	 // optional
    String PAGENUM		= "PAGENUM"; // optional
    String RECID		= "RECID"; // optional

	// list of attributes added by the virtual lock table by asking
	// the lock for its compatibility space and count
	String XACTID		= "XID";
    String LOCKCOUNT	= "LOCKCOUNT";

	// list of attributes added by the virtual lock table by asking
	// the lock qualifier
	String LOCKMODE		= "MODE";

	// list of attributes to be supplied the virtual lock table by looking at 
	// the lock table
	String STATE		= "STATE";
	String LOCKOBJ		= "LOCKOBJ";

	// list of attributes filled in by virtual lock table with help from data
	// dictionary 
	String TABLENAME	= "TABLENAME";
	String INDEXNAME	= "INDEXNAME";
	String TABLETYPE	= "TABLETYPE";

}
