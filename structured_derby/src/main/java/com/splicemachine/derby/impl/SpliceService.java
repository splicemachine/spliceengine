package com.splicemachine.derby.impl;


import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.EngineType;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.io.StorageFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.derby.impl.store.access.PropertyConglomerate2;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;

public class SpliceService implements PersistentService {
	protected static final String TYPE = "splice";
	private static Logger LOG = Logger.getLogger(SpliceService.class);
	public SpliceService() {
		SpliceLogUtils.trace(LOG,"instantiated");
	    Thread.currentThread().setContextClassLoader(HBaseConfiguration.class.getClassLoader());
	}
	
	public String getType() {
		SpliceLogUtils.trace(LOG,"getType %s",TYPE);
		return TYPE;
	}

	@SuppressWarnings("rawtypes")
	public Enumeration getBootTimeServices() {
		SpliceLogUtils.trace(LOG,"getBootTimeServices");
		return null;
	}

	public Properties getServiceProperties(String serviceName, Properties defaultProperties) throws StandardException {
		SpliceLogUtils.trace(LOG,"getServiceProperties serviceName: %s, defaultProperties %s",serviceName, defaultProperties);
//		Properties service = new Properties(SpliceUtils.getAllProperties(defaultProperties));
		Properties service = new Properties(defaultProperties);
		service.setProperty(Property.SERVICE_PROTOCOL,"org.apache.derby.database.Database");
		service.setProperty(EngineType.PROPERTY,Integer.toString(getEngineType()));
		service.setProperty(DataDictionary.CORE_DATA_DICTIONARY_VERSION,"10.9");
//		service.setProperty(Property.REQUIRE_AUTHENTICATION_PARAMETER, "true");
//		service.setProperty("derby.language.logQueryPlan", "true"); // unclear of this...
		if (LOG.isTraceEnabled()) {
			LOG.trace("getServiceProperties actual properties serviceName" + serviceName + ", properties " + service);
		}
		return service;
	}

	public void saveServiceProperties(String serviceName, StorageFactory storageFactory, Properties properties, boolean replace) throws StandardException {
		SpliceAccessManager test;
		SpliceLogUtils.trace(LOG,"saveServiceProperties with storageFactory serviceName: %s, properties %s, replace %s",serviceName, properties, replace);
		for (Object key :properties.keySet()) {
			System.out.println("saveServiceProperties with stf " + key + " : " + properties.getProperty((String)key));
//			if (!SpliceUtils.propertyExists((String)key)) {
//				SpliceUtils.addProperty((String)key, (String)properties.getProperty((String)key));
//			}
		}
	}

    public void saveServiceProperties(String serviceName,Properties properties) throws StandardException {
		SpliceLogUtils.trace(LOG,"saveServiceProperties serviceName: %s, properties %s",serviceName, properties);
		for (Object key :properties.keySet()) {
			System.out.println("saveServiceProperties " + key + " : " + properties.getProperty((String)key));
//			if (!SpliceUtils.propertyExists((String)key)) {
//				SpliceUtils.addProperty((String)key, (String)properties.getProperty((String)key));
//			}
		}
	}

	public String createServiceRoot(String name, boolean deleteExisting) throws StandardException {
		SpliceLogUtils.trace(LOG,"createServiceRoot serviceName: %s",name);
		return null;
	}

	public boolean removeServiceRoot(String serviceName) {
		SpliceLogUtils.trace(LOG,"removeServiceRoot serviceName: %s",serviceName);
		return false;
	}

	public String getCanonicalServiceName(String name) {
		SpliceLogUtils.trace(LOG,"getCanonicalServiceName name: %s",name);
		return name;
	}

	public String getUserServiceName(String serviceName) {
		SpliceLogUtils.trace(LOG,"getUserServiceName name: %s",serviceName);
		return null;
	}

	public boolean isSameService(String serviceName1, String serviceName2) {
		SpliceLogUtils.trace(LOG,"isSameService %s = %s",serviceName1,serviceName2);
		return serviceName1.equals(serviceName2);
	}

	public boolean hasStorageFactory() {
		SpliceLogUtils.trace(LOG,"hasStorageFactory ");
		return false;
	}

	public StorageFactory getStorageFactoryInstance(boolean useHome, String databaseName, String tempDirName, String uniqueName) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG,"getStorageFactoryInstance ");
		return null;
	}

    protected int getEngineType() {
		SpliceLogUtils.trace(LOG,"getEngineType");
        return EngineType.STANDALONE_DB;
    }
}

