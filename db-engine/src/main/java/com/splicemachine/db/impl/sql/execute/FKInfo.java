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

import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.RowLocation;

import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.FormatIdUtil;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.Formatable;

import java.io.StreamCorruptedException;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Vector;

/**
 * This is a simple class used to store the run time information
 * about a foreign key.  Used by DML to figure out what to
 * check.
 *
 */
public class FKInfo implements Formatable {

    /********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.  OR, since this is something that is used
	**	in stored prepared statements, it is ok to change it
	**	if you make sure that stored prepared statements are
	**	invalidated across releases.
	**
	********************************************************/

	public static final int FOREIGN_KEY = 1;
	public static final int REFERENCED_KEY = 2;

	/*
	** See the constructor for the meaning of these fields
	*/
	public String				fkConstraintNames[];
	public String				tableName;
	public int					type;
	public UUID					refUUID;
	public long					refConglomNumber;
	public UUID[]				fkUUIDs;
	public long[]				fkConglomNumbers;
	public boolean[]			fkIsSelfReferencing;
	public int[]				colArray;
	public int					stmtType;
	public RowLocation			rowLocation;
	public int[] 	            raRules;

	/**
	 * Niladic constructor for Formattable
	 */
	public FKInfo() {}

	/**
	 * Consructor for FKInfo
	 *
	 * @param fkConstraintNames the foreign key constraint names
	 * @param tableName	the name of the table being modified
	 * @param stmtType	the type of the statement: e.g. StatementType.INSERT
	 * @param type either FKInfo.REFERENCED_KEY or FKInfo.FOREIGN_KEY
	 * @param refUUID UUID of the referenced constraint
	 * @param refConglomNumber congomerate number of the referenced key
	 * @param fkUUIDs an array of fkUUIDs of backing indexes.  if
	 *			FOREIGN_KEY, then just one element, the backing
	 *			index of the referrenced keys.  if REFERENCED_KEY,
	 *			then all the foreign keys
	 * @param fkConglomNumbers array of conglomerate numbers, corresponds
	 *			to fkUUIDs
	 * @param fkIsSelfReferencing array of conglomerate booleans indicating
	 *			whether the fk references a key in the same table
	 * @param colArray map of columns to the base row that DML
	 * 			is changing.  1 based.  Note that this maps the
	 *			constraint index to a row in the target table of
	 *			the current dml operation.
	 * @param rowLocation a row location template for the target table
	 *			used to pass in a template row to tc.openScan()
	 */
	public FKInfo(
					String[]			fkConstraintNames,
					String				tableName,
					int					stmtType,
					int					type,
					UUID				refUUID,
					long				refConglomNumber,
					UUID[]				fkUUIDs,
					long[]				fkConglomNumbers,
					boolean[]			fkIsSelfReferencing,
					int[]				colArray,
					RowLocation			rowLocation,
					int[]               raRules
					)
	{
		this.fkConstraintNames = fkConstraintNames;
		this.tableName = tableName;
		this.stmtType = stmtType;
		this.type = type;
		this.refUUID = refUUID;
		this.refConglomNumber = refConglomNumber;
		this.fkUUIDs = fkUUIDs;
		this.fkConglomNumbers = fkConglomNumbers;
		this.fkIsSelfReferencing = fkIsSelfReferencing;
		this.colArray = colArray;
		this.rowLocation = rowLocation;
		this.raRules = raRules;

		if (SanityManager.DEBUG)
		{
			if (fkUUIDs.length != fkConglomNumbers.length)
			{
				SanityManager.THROWASSERT("number of ForeignKey UUIDS ("+fkUUIDs.length+
										") doesn't match the number of conglomerate numbers"+
										" ("+fkConglomNumbers.length+")");
			}
			if (type == FOREIGN_KEY)
			{
				SanityManager.ASSERT(fkUUIDs.length == 1, "unexpected number of fkUUIDs for a foreign key, should only have the uuid of the key it references");
			}
			else if (type == REFERENCED_KEY)
			{
				SanityManager.ASSERT(fkUUIDs.length >= 1, "too few fkUUIDs for a referenced key, expect at least one foreign key");
			}
			else
			{
				SanityManager.THROWASSERT("bad type: "+type);
			}
		}
	}

	/**
	 * Comb through the FKInfo structures and pick out the
	 * ones that have columns that intersect with the input
	 * columns.
	 *
 	 * @param fkInfo	        array of fkinfos
	 * @param cols	            array of columns
	 * @param addAllTypeIsFK	take all with type == FORIEGN_KEY
	 *
	 * @return array of relevant fkinfos
	 */
	public static FKInfo[] chooseRelevantFKInfos
	(	
		FKInfo[] 	fkInfo, 
		int[] 		cols,
		boolean		addAllTypeIsFK)
	{
		if (fkInfo == null)
		{
			return null;
		}

		Vector<FKInfo> newfksVector = new Vector<>();
		FKInfo[] newfks = null;

		/*
		** For each FKInfo
		*/
        for (FKInfo aFkInfo : fkInfo) {
            if (addAllTypeIsFK && (aFkInfo.type == FOREIGN_KEY)) {
                newfksVector.addElement(aFkInfo);
                continue;
            }

            int fkcollen = aFkInfo.colArray.length;
            for (int fkCols = 0; fkCols < fkcollen; fkCols++) {
                for (int col : cols) {
                    /*
                    ** If any column intersects, the FKInfo is
					** relevant.
					*/
                    if (aFkInfo.colArray[fkCols] == col) {
                        newfksVector.addElement(aFkInfo);

                        // go to the next fk
                        fkCols = fkcollen;
                        break;
                    }
                }
            }
        }

		
		/*
		** Now convert the vector into an array.
		*/
		int size = newfksVector.size();
		if (size > 0) {
			newfks = new FKInfo[size];
			for (int i = 0; i < size; i++)
			{
				newfks[i] = newfksVector.elementAt(i);
			}
		}
		return newfks;
	}
		
	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////

	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
    @Override
	public void writeExternal(ObjectOutput out) throws IOException
	{
		/*
		** Row locations cannot be written unless they
		** have a valid value.  So we'll just write out
		** the format id, and create a new RowLocation
		** when we read it back in.
		*/
		FormatIdUtil.writeFormatIdInteger(out, rowLocation.getTypeFormatId());

		out.writeObject(tableName);
		out.writeInt(type);
		out.writeInt(stmtType);
		out.writeObject(refUUID);
		out.writeLong(refConglomNumber);

		ArrayUtil.writeArray(out, fkConstraintNames);
		ArrayUtil.writeArray(out, fkUUIDs);
		ArrayUtil.writeLongArray(out, fkConglomNumbers);
		ArrayUtil.writeBooleanArray(out, fkIsSelfReferencing);
		ArrayUtil.writeIntArray(out, colArray);
		ArrayUtil.writeIntArray(out, raRules);
		
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
    @Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		try
		{
			/*
			** Create a new RowLocation from the format id.
			*/
			int formatid = FormatIdUtil.readFormatIdInteger(in);
			rowLocation = (RowLocation)Monitor.newInstanceFromIdentifier(formatid);
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(rowLocation != null, "row location is null in readExternal");
			}

			tableName = (String)in.readObject();
			type = in.readInt();
			stmtType = in.readInt();
			refUUID = (UUID)in.readObject();
			refConglomNumber = in.readLong();

			fkConstraintNames = new String[ArrayUtil.readArrayLength(in)];
			ArrayUtil.readArrayItems(in, fkConstraintNames);

			fkUUIDs = new UUID[ArrayUtil.readArrayLength(in)];
			ArrayUtil.readArrayItems(in, fkUUIDs);

			fkConglomNumbers = ArrayUtil.readLongArray(in);
			fkIsSelfReferencing = ArrayUtil.readBooleanArray(in);
			colArray = ArrayUtil.readIntArray(in);
			raRules = ArrayUtil.readIntArray(in);
		}
		catch (StandardException exception)
		{
			throw new StreamCorruptedException(exception.toString());
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FK_INFO_V01_ID; }

	//////////////////////////////////////////////////////////////
	//
	// Misc
	//
	//////////////////////////////////////////////////////////////

    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuilder str = new StringBuilder();
			str.append("\nTableName:\t\t\t");
			str.append(tableName);

			str.append("\ntype:\t\t\t\t");
			str.append((type == FOREIGN_KEY) ? "FOREIGN_KEY" : "REFERENCED_KEY");

			str.append("\nReferenced Key UUID:\t\t").append(refUUID);
			str.append("\nReferenced Key ConglomNum:\t").append(refConglomNumber);

			str.append("\nForeign Key Names:\t\t(");
			for (int i = 0; i < fkUUIDs.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(fkConstraintNames[i]);
			}
			str.append(")");

			str.append("\nForeign Key UUIDS:\t\t(");
			for (int i = 0; i < fkUUIDs.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(fkUUIDs[i]);
			}
			str.append(")");

			str.append("\nForeign Key Conglom Nums:\t(");
			for (int i = 0; i < fkConglomNumbers.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(fkConglomNumbers[i]);
			}
			str.append(")");
		
			str.append("\nForeign Key isSelfRef:\t\t(");
			for (int i = 0; i < fkIsSelfReferencing.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(fkIsSelfReferencing[i]);
			}
			str.append(")");
		
			str.append("\ncolumn Array:\t\t\t(");
			for (int i = 0; i < colArray.length; i++)
			{
				if (i > 0)
					str.append(",");
			
				str.append(colArray[i]);
			}
			str.append(")\n");

			return str.toString();
		}
		else
		{
			return "";
		}
	}
}
