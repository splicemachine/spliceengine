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

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.services.io.DataInputUtil;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecCursorTableReference;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.types.ProtobufUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Arrays;

/**
 * A basic holder for information about cursors
 * for execution.
 * 
 */
@SuppressFBWarnings({"EI_EXPOSE_REP2", "DMI_NONSERIALIZABLE_OBJECT_WRITTEN" /*todo: fix*/})
public class CursorInfo
	implements Formatable
{

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

	ExecCursorTableReference	targetTable; 
	ResultColumnDescriptor[]	targetColumns; 
	String[] 					updateColumns; 
	int 						updateMode;

	/**
	 * Niladic constructor for Formatable
	 */
	public CursorInfo()
	{
	}

	/**
	 *
	 */
	public CursorInfo
	(
		int							updateMode,
		ExecCursorTableReference	targetTable,
		ResultColumnDescriptor[]	targetColumns,
		String[]					updateColumns
	)
	{
		this.updateMode = updateMode;
		this.targetTable = targetTable;
		this.targetColumns = targetColumns;
		this.updateColumns = updateColumns;
	}

	public CursorInfo(CatalogMessage.CursorInfo cursorInfo) throws IOException {
		init(cursorInfo);
	}

	private void init(CatalogMessage.CursorInfo cursorInfo) throws IOException {
		updateMode = cursorInfo.getUpdateMode();
		targetTable = ProtobufUtils.fromProtobuf(cursorInfo.getTargetTable());
		int len = cursorInfo.getTargetColumnsCount();
		if (len != 0) {
			targetColumns = new ResultColumnDescriptor[len];
			for (int i = 0; i < len; ++i) {
				targetColumns[i] = ProtobufUtils.fromProtobuf(cursorInfo.getTargetColumns(i));
			}
		}
		len = cursorInfo.getUpdateColumnsCount();
		if (len != 0) {
			updateColumns = new String[len];
			for (int i = 0; i < len; ++i) {
				updateColumns[i] = cursorInfo.getUpdateColumns(i);
			}
		}
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
	public void writeExternal(ObjectOutput out) throws IOException {
		if (DataInputUtil.shouldWriteOldFormat()) {
			writeExternalOld(out);
		}
		else {
			writeExternalNew(out);
		}
	}

	protected void writeExternalNew(ObjectOutput out) throws IOException {
		CatalogMessage.CursorInfo cursorInfo = toProtobuf();
		ArrayUtil.writeByteArray(out, cursorInfo.toByteArray());
	}

	public CatalogMessage.CursorInfo toProtobuf() {
		CatalogMessage.CursorInfo.Builder builder = CatalogMessage.CursorInfo.newBuilder();
		builder.setUpdateMode(updateMode);
		if (targetTable != null) {
			builder.setTargetTable(targetTable.toProtobuf());
		}
		if (targetColumns != null) {
			for (int i = 0; i < targetColumns.length; ++i) {
				builder.addTargetColumns(i, targetColumns[i].toProtobuf());
			}
		}
		if (updateColumns != null) {
			builder.addAllUpdateColumns(Arrays.asList(updateColumns));
		}
		return builder.build();
	}

	protected void writeExternalOld(ObjectOutput out) throws IOException
	{
		out.writeInt(updateMode);
		out.writeObject(targetTable);
		ArrayUtil.writeArray(out, targetColumns);
		ArrayUtil.writeArray(out, updateColumns);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
			throws IOException, ClassNotFoundException {
		if (DataInputUtil.shouldReadOldFormat()) {
			readExternalOld(in);
		}
		else {
			readExternalNew(in);
		}
	}

	public void readExternalNew(ObjectInput in)
			throws IOException, ClassNotFoundException {
		byte[] bs = ArrayUtil.readByteArray(in);
		CatalogMessage.CursorInfo cursorInfo = CatalogMessage.CursorInfo.parseFrom(bs);
		init(cursorInfo);
	}

	public void readExternalOld(ObjectInput in) throws IOException, ClassNotFoundException {
		updateMode = in.readInt();
		targetTable = (ExecCursorTableReference)in.readObject();
		int len = ArrayUtil.readArrayLength(in);
		if (len != 0)
		{
			targetColumns = new ResultColumnDescriptor[len];
			ArrayUtil.readArrayItems(in, targetColumns);
		}
		len = ArrayUtil.readArrayLength(in);
		if (len != 0)
		{
			updateColumns = new String[len];
			ArrayUtil.readArrayItems(in, updateColumns);
		}
	}

	public static CursorInfo fromProtobuf(CatalogMessage.CursorInfo cursorInfo) throws IOException {
		return new CursorInfo(cursorInfo);
	}
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.CURSOR_INFO_V01_ID; }

	public String toString()
	{
		StringBuilder strbuf = new StringBuilder();

		strbuf.append("CursorInfo" + "\n\tupdateMode: ").append(updateMode).append("\n\ttargetTable: ").append(targetTable).append("\n\tupdateColumns: ");

		if (updateColumns == null)
		{
			strbuf.append("NULL\n");
		}
		else
		{
			strbuf.append("{");
			for (int i = 0; i < updateColumns.length; i++)
			{
				if (i > 0)
					strbuf.append(",");
				strbuf.append(updateColumns[i]);
			}
			strbuf.append(")\n");
		}

		strbuf.append("\tTargetColumnDescriptors: \n");
		if (targetColumns == null)
		{
			strbuf.append("NULL");
		}
		else
		{
			for (ResultColumnDescriptor targetColumn : targetColumns) {
				strbuf.append(targetColumn);
			}
			strbuf.append("\n");
		}
		return strbuf.toString();
	}
}
