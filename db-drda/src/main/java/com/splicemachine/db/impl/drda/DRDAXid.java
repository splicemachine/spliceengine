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
 * This class provides an Xid implementation for Network Server XA
 */

package com.splicemachine.db.impl.drda;
import javax.transaction.xa.Xid;

class DRDAXid implements Xid
{

	private final int format_id;
	private final byte[] global_id;
	private final byte[] branch_id;


	DRDAXid(int formatid, byte[] globalid, byte[] branchid)
	{

		format_id = formatid;
		global_id = globalid;
		branch_id = branchid;
		
	}

    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
	 * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
	 * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier()
    {
        return(branch_id);
    }

	public String toString()
	{

		return "{DRDAXid: " +
            "formatId("     + format_id   + "), " +
            "globalTransactionId(" +  convertToHexString(global_id) + ")" +
            "branchQualifier(" +  convertToHexString(branch_id) + ")";
	}


	/**
	 * convert byte array to a Hex string
	 * 
	 * @param buf buffer to  convert
	 * @return hex string representation of byte array
	 */
	private static String convertToHexString(byte [] buf)
	{
		if (buf == null)
			return null;
		StringBuilder str = new StringBuilder();
		str.append("0x");
		String val;
		int byteVal;
		for (int i = 0; i < buf.length; i++)
		{
			byteVal = buf[i] & 0xff;
			val = Integer.toHexString(byteVal);
			if (val.length() < 2)
				str.append("0");
			str.append(val);
		}
		return str.toString();
	}
}









