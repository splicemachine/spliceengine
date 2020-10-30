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

package com.splicemachine.db.catalog.types;
import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.DataInputUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.Types;

public class DecimalTypeIdImpl extends BaseTypeIdImpl
{
	/**
	 * Public niladic constructor. Needed for Serializable interface to work.
	 *
	 */
	public	DecimalTypeIdImpl() { super(); }

	/* this class is needed because writeexternal for this class stores
	   extra information; when the object is sent over the wire the niladic
	   constructor is first called and then we call the readExternal method. 
	   the readExternal needs to know the formatId atleast for decimal types
	   to read the extra information.
	*/
	public DecimalTypeIdImpl(boolean isNumeric) 
	{
		super(StoredFormatIds.DECIMAL_TYPE_ID_IMPL);
        if (isNumeric)
            setNumericType();
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
	public void readExternal( ObjectInput in )
			throws IOException, ClassNotFoundException {
		if (DataInputUtil.isOldFormat()) {
			readExternalOld(in);
		}
		else {
			readExternalNew(in);
		}
	}

	public void readExternalNew( ObjectInput in )
			throws IOException, ClassNotFoundException
	{
		byte[] bs = ArrayUtil.readByteArray(in);
		ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
		extensionRegistry.add(CatalogMessage.DecimalTypeIdImpl.decimalTypeIdImpl);
		CatalogMessage.BaseTypeIdImpl baseTypeId = CatalogMessage.BaseTypeIdImpl.parseFrom(bs, extensionRegistry);
		init(baseTypeId);
	}

	@Override
	protected void init(CatalogMessage.BaseTypeIdImpl baseTypeId ) {
		super.init(baseTypeId);
		CatalogMessage.DecimalTypeIdImpl decimalTypeId =
				baseTypeId.getExtension(CatalogMessage.DecimalTypeIdImpl.decimalTypeIdImpl);
		boolean isNumeric = decimalTypeId.getIsNumeric();
		if (isNumeric)
		{
			setNumericType();
		}
	}

	public void readExternalOld( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		boolean isNumeric = in.readBoolean();

		super.readExternalOld(in);

		if (isNumeric)
		{
			setNumericType();
		}

	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	@Override
	public void writeExternal( ObjectOutput out )
			throws IOException
	{
		CatalogMessage.BaseTypeIdImpl typeId = toProtobuf();
		byte[] bs = typeId.toByteArray();
		ArrayUtil.writeByteArray(out, bs);
	}

	protected void setNumericType()
	{
		unqualifiedName = "NUMERIC";
		JDBCTypeId = Types.NUMERIC;
	}

	@Override
	public CatalogMessage.BaseTypeIdImpl toProtobuf() {
		CatalogMessage.DecimalTypeIdImpl decimalTypeId = CatalogMessage.DecimalTypeIdImpl.newBuilder()
				.setIsNumeric(getJDBCTypeId() == Types.NUMERIC)
				.build();

		CatalogMessage.BaseTypeIdImpl baseTypeId = super.toProtobuf();
		CatalogMessage.BaseTypeIdImpl.Builder builder = CatalogMessage.BaseTypeIdImpl.newBuilder().mergeFrom(baseTypeId);
		builder.setType(CatalogMessage.BaseTypeIdImpl.Type.DecimalTypeIdImpl)
				.setExtension(CatalogMessage.DecimalTypeIdImpl.decimalTypeIdImpl, decimalTypeId);
		return builder.build();
	}
}
