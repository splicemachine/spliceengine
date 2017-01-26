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

import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.catalog.DefaultInfo;
import com.splicemachine.db.catalog.UUID;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	This is the Column descriptor that is passed from Compilation to Execution
 *	for CREATE TABLE statements.
 *
 *	@version 0.1
 */

public class ColumnInfo implements Formatable {
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
	**	method.
	**
	********************************************************/

	public  int							action;
	public	String						name;
	public	DataTypeDescriptor			dataType;
	public	DefaultInfo					defaultInfo;
    public    ProviderInfo[]            providers;
	public	DataValueDescriptor			defaultValue;
	public	UUID						newDefaultUUID;
	public	UUID						oldDefaultUUID;
	// autoinc columns.
	public long 						autoincStart;
	public long 						autoincInc;
	//if this is an autoincrement column, then following variable will have CREATE or
	//MODIFY_COLUMN_DEFAULT_RESTART or MODIFY_COLUMN_DEFAULT_INCREMENT. Otherwise,
	//this variable will be set to -1.
	public long 						autoinc_create_or_modify_Start_Increment = -1;
	public int 							partitionPosition;

	//This indicates column is for CREATE TABLE
	public static final int CREATE					= 0;
	public static final int DROP					= 1;
	public static final int MODIFY_COLUMN_TYPE      = 2;
	public static final int MODIFY_COLUMN_CONSTRAINT = 3;
	public static final int MODIFY_COLUMN_CONSTRAINT_NOT_NULL = 4;
	//This indicates column is for ALTER TABLE to change the start value of autoinc column 
	public static final int MODIFY_COLUMN_DEFAULT_RESTART	= 5;
	//This indicates column is for ALTER TABLE to change the increment value of autoinc column 
	public static final int MODIFY_COLUMN_DEFAULT_INCREMENT	= 6;
	public static final int MODIFY_COLUMN_DEFAULT_VALUE	= 7;
	public static final int MODIFY_COLUMN_PARTITION_POSITION	= 8;

	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	ColumnInfo() {}

	/**
	 *	Make one of these puppies.
	 *
	 *  @param name			Column name.
	 *  @param dataType		Column type.
	 *  @param defaultValue	Column default value.
	 *  @param defaultInfo	Column default info.
	 *  @param providers   Array of providers that this column depends on.
	 *  @param newDefaultUUID	New UUID for default.
	 *  @param oldDefaultUUID	Old UUID for default.
	 *	@param action		Action (create, modify default, etc.)
	 * 	@param autoincStart Start of autoincrement values.
	 *  @param autoincInc	Increment of autoincrement values-- if parameter
	 *						is 0, it implies that this is not an autoincrement
	 *						value.
	 */
	public	ColumnInfo(
		               String						name,
					   DataTypeDescriptor			dataType,
					   DataValueDescriptor			defaultValue,
					   DefaultInfo					defaultInfo,
					   ProviderInfo[]					providers,
					   UUID							newDefaultUUID,
					   UUID							oldDefaultUUID,
					   int							action,
					   long							autoincStart,
					   long							autoincInc,
					   long							autoinc_create_or_modify_Start_Increment,
					   int 							partitionPosition)
	{
		this.name = name;
		this.dataType = dataType;
		this.defaultValue = defaultValue;
		this.defaultInfo = defaultInfo;
        this.providers = providers;
		this.newDefaultUUID = newDefaultUUID;
		this.oldDefaultUUID = oldDefaultUUID;
		this.action = action;
		this.autoincStart = autoincStart;
		this.autoincInc = autoincInc;
		this.autoinc_create_or_modify_Start_Increment = autoinc_create_or_modify_Start_Increment;
		this.partitionPosition = partitionPosition;
	}

	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException {

		name = in.readUTF();
		dataType = (DataTypeDescriptor) in.readObject();
		defaultValue = (DataValueDescriptor) in.readObject();
		defaultInfo = (DefaultInfo) in.readObject();
		newDefaultUUID = (UUID) in.readObject();
		oldDefaultUUID = (UUID)in.readObject();
		action = in.readInt();
		autoincStart = in.readLong();
		autoincInc = in.readLong();
		providers = new ProviderInfo[in.readInt()];
		for (int i = 0; i< providers.length;i++) {
			providers[i] = (ProviderInfo) in.readObject();
		}
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException {
		out.writeUTF(name);
		out.writeObject(dataType);
		out.writeObject(defaultValue);
		out.writeObject(defaultInfo);
		out.writeObject(newDefaultUUID);
		out.writeObject(oldDefaultUUID);
		out.writeInt(action);
		out.writeLong(autoincStart);
		out.writeLong(autoincInc);
		int providerLength = providers==null?0:providers.length;
		out.writeInt(providerLength);
		for (int i = 0; i< providerLength;i++) {
			out.writeObject(providers[i]);
		}
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.COLUMN_INFO_V02_ID; }

	/*
	  Object methods.
	  */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String	traceName;
			String  traceDataType;
			String  traceDefaultValue;
			String  traceDefaultInfo;
			String  traceNewDefaultUUID;
			String  traceOldDefaultUUID;
			String  traceAction;
			String  traceAI;
			if (name == null)
			{
				traceName = "name: null ";
			}
			else
			{
				traceName = "name: "+name+" ";
			}

			if (dataType == null)
			{
				traceDataType = "dataType: null ";
			}
			else
			{
				traceDataType = "dataType: "+dataType+" ";
			}

			if (defaultValue == null)
			{
				traceDefaultValue = "defaultValue: null ";
			}
			else
			{
				traceDefaultValue = "defaultValue: "+defaultValue+" ";
			}

			if (defaultInfo == null)
			{
				traceDefaultInfo = "defaultInfo: null ";
			}
			else
			{
				traceDefaultInfo = "defaultInfo: "+defaultInfo+" ";
			}

			if (newDefaultUUID == null)
			{
				traceNewDefaultUUID = "newDefaultUUID: null ";
			}
			else
			{
				traceNewDefaultUUID = "newDefaultUUID: "+newDefaultUUID+" ";
			}

			if (oldDefaultUUID == null)
			{
				traceOldDefaultUUID = "oldDefaultUUID: null ";
			}
			else
			{
				traceOldDefaultUUID = "oldDefaultUUID: "+oldDefaultUUID+" ";
			}

			traceAction = "action: "+action+" ";

			if (autoincInc != 0)
			{
				traceAI = "autoincrement, start: " + autoincStart +
					" increment:" + autoincInc;
			}
			else
			{
				traceAI = "NOT autoincrement";
			}
			return "ColumnInfo: ("+traceName+traceDataType+traceDefaultValue+
							   traceDefaultInfo+traceNewDefaultUUID+traceOldDefaultUUID+traceAction+traceAI+")";
		}
		else
		{
			return "";
		}
	}
}
