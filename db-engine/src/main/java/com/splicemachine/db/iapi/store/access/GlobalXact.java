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

package com.splicemachine.db.iapi.store.access;


/**

This abstract class represents a global transaction id which can be tested
for equality against other transaction ids, which can be hashed into a
hash table, and which can be output as a string. 
<P>
This class has 2 direct subclasses. 
<UL>
<LI> com.splicemachine.db.iapi.store.access.xa.XAXactId :
this class is a specific implementation of the JTA Xid interface
<LI> com.splicemachine.db.impl.store.access.GlobalXactId :
this class represents internal Derby transaction ids
</UL>
<P>
The main reason for this class is to ensure that equality etc. works in a
consistent way across both subclasses. 
**/

public abstract class GlobalXact {
    
    /**************************************************************************
     * Protected Fields of the class
     **************************************************************************
     */
    protected int     format_id;
    protected byte[]  global_id;
    protected byte[]  branch_id;

    public boolean equals(Object other) 
    {
		if (other == this)
			return true;

		if (other instanceof GlobalXact) {
	
			GlobalXact other_xact = (GlobalXact) other;
		
			return(
				   java.util.Arrays.equals(
									other_xact.global_id,
									this.global_id)          &&
				   java.util.Arrays.equals(
									other_xact.branch_id,
									this.branch_id)          &&
				   other_xact.format_id == this.format_id);
		
	    }

		return false;	
    }

    public String toString()
    {
		StringBuilder globalhex = new StringBuilder();
		StringBuilder branchhex = new StringBuilder();
		if (global_id != null) 
	    {
			int mask = 0;
            for (byte aGlobal_id : global_id) {
                mask = (aGlobal_id & 0xFF);
                if (mask < 16) {
                    globalhex.append("0").append(Integer.toHexString(mask));
                } else {
                    globalhex.append(Integer.toHexString(mask));
                }
            }
	    }
	
		if (branch_id != null)
	    {
			int mask = 0;
            for (byte aBranch_id : branch_id) {
                mask = (aBranch_id & 0xFF);
                if (mask < 16) {
                    branchhex.append("0").append(Integer.toHexString(mask));
                } else {
                    branchhex.append(Integer.toHexString(mask));
                }
            }
	    }

		return("(" + format_id + "," + globalhex + "," + branchhex + ")");
	
    }


    /**
       Provide a hashCode which is compatable with the equals() method.
       
       @see java.lang.Object#hashCode
    **/
    public int hashCode()
    {
		// make sure hash does not overflow int, the only unknown is
		// format_id.  Lop off top bits.
		int hash = global_id.length + branch_id.length + (format_id & 0xFFFFFFF);

        for (byte aGlobal_id : global_id) {
            hash += aGlobal_id;
        }
        for (byte aBranch_id : branch_id) {
            hash += aBranch_id;
        }
	
		return(hash);
    }
    
}



