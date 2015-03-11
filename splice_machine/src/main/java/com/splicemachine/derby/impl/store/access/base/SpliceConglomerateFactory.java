package com.splicemachine.derby.impl.store.access.base;

import java.util.Properties;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.store.access.conglomerate.ConglomerateFactory;

public abstract class SpliceConglomerateFactory implements ConglomerateFactory, ModuleControl, ModuleSupportable {
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
