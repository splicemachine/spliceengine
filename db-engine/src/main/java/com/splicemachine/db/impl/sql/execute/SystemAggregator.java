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

package com.splicemachine.db.impl.sql.execute;

import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.DataInputUtil;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.ProtobufUtils;
import com.splicemachine.db.impl.sql.CatalogMessage;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Abstract aggregator that is extended by all internal
 * (system) aggregators.
 *
 */
abstract class SystemAggregator implements ExecAggregator
{

    protected boolean eliminatedNulls;


	public boolean didEliminateNulls() {
		return eliminatedNulls;
	}

	public void accumulate(DataValueDescriptor addend, Object ga) 
		throws StandardException
	{
		if ((addend == null) || addend.isNull()) {
			eliminatedNulls = true;
			return;
		}

		this.accumulate(addend);
	}

	protected abstract void accumulate(DataValueDescriptor addend)
		throws StandardException;

	public String toString()
	{
		try
		{
			return super.toString() + "[" + getResult().getString() + "]";
		}
		catch (Exception e)
		{
			return e.getMessage();
		}
	}

	@Override
	public boolean isUserDefinedAggregator() {
		return false;
	}

	/////////////////////////////////////////////////////////////
	//
	// EXTERNALIZABLE INTERFACE
	//
	/////////////////////////////////////////////////////////////
	@Override
	public void writeExternal( ObjectOutput out ) throws IOException {
		if (DataInputUtil.shouldWriteOldFormat()) {
			writeExternalOld(out);
		}
		else {
			writeExternalNew(out);
		}
	}

	protected void writeExternalNew(ObjectOutput out) throws IOException {
		CatalogMessage.SystemAggregator systemAggregator = toProtobufBuilder().build();
		ArrayUtil.writeByteArray(out, systemAggregator.toByteArray());
	}

	protected void writeExternalOld(ObjectOutput out) throws IOException {
		out.writeBoolean(eliminatedNulls);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
		if (DataInputUtil.shouldReadOldFormat()) {
			readExternalOld(in);
		}
		else {
			readExternalNew(in);
		}
	}

	protected void readExternalNew(ObjectInput in) throws IOException, ClassNotFoundException {
		byte[] bs = ArrayUtil.readByteArray(in);
		ExtensionRegistry extensionRegistry = ProtobufUtils.getExtensionRegistry();
		CatalogMessage.SystemAggregator systemAggregator =
				CatalogMessage.SystemAggregator.parseFrom(bs, extensionRegistry);
		init(systemAggregator);
	}

	protected void readExternalOld(ObjectInput in) throws IOException, ClassNotFoundException {
		eliminatedNulls = in.readBoolean();
	}

	protected CatalogMessage.SystemAggregator.Builder toProtobufBuilder() throws IOException{
		CatalogMessage.SystemAggregator.Builder builder = CatalogMessage.SystemAggregator.newBuilder()
				.setEliminatedNulls(eliminatedNulls);
		return builder;
	}

	protected void init(CatalogMessage.SystemAggregator systemAggregator) throws IOException, ClassNotFoundException {
		eliminatedNulls = systemAggregator.getEliminatedNulls();
	}
}
