/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
		String impl = startParams.getProperty("derby.access.Record.type");
		if (impl == null)
			return false;
		return supportsImplementation(impl);
	}
	
	abstract protected String getImplementationID();
	abstract protected String getFormatUUIDString();
	abstract public int getConglomerateFactoryId();
	
	
}
