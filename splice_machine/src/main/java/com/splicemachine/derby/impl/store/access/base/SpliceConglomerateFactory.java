/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.store.access.base;

import java.util.Properties;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.store.access.conglomerate.ConglomerateFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;

public abstract class SpliceConglomerateFactory implements ConglomerateFactory, ModuleControl, ModuleSupportable {
	protected TxnOperationFactory operationFactory;
	protected PartitionFactory partitionFactory;
	protected UUID formatUUID;
	public SpliceConglomerateFactory() {
	
	}

	public void	stop() {
	
	}

	protected UUID getformatUUID() {
		return formatUUID;
	}
	
	public void	boot(boolean create, Properties startParams) throws StandardException {
		UUIDFactory uuidFactory = Monitor.getMonitor().getUUIDFactory();
		formatUUID = uuidFactory.recreateUUID(getFormatUUIDString());
		SIDriver driver=SIDriver.driver();
		operationFactory =driver.getOperationFactory();
		partitionFactory = driver.getTableFactory();
	}

	public boolean supportsImplementation(String implementationId) {
		return implementationId.equals(getImplementationID());
	}

	public String primaryImplementationType() {
		return getImplementationID();
	}
	
	public boolean supportsFormat(UUID formatid) {
		return formatid.equals(formatUUID);
	}

	public Properties defaultProperties() {
		return new Properties();
	}

	public UUID primaryFormat() {
		return formatUUID;
	}
	public boolean canSupport(Properties startParams) {
		String impl = startParams.getProperty("derby.access.Conglomerate.type");
		if (impl == null)
			return false;
		return supportsImplementation(impl);
	}
	
	abstract protected String getImplementationID();
	abstract protected String getFormatUUIDString();
	abstract public int getConglomerateFactoryId();
	
	
}
