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

package com.splicemachine.derby.impl;


import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.EngineType;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.monitor.PersistentService;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.PropertyManagerService;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

public class SpliceService implements PersistentService {
	protected static final String TYPE = "splice";
	private static Logger LOG = Logger.getLogger(SpliceService.class);
	private PropertyManager propertyManager;

	public SpliceService() {
		SpliceLogUtils.trace(LOG,"instantiated");
		propertyManager = PropertyManagerService.loadPropertyManager();
//	    Thread.currentThread().setContextClassLoader(HBaseConfiguration.class.getClassLoader());
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

	@Override
	public Properties getServiceProperties(String serviceName, Properties defaultProperties) throws StandardException {
		Properties service = new Properties(defaultProperties);
		try {
			Set<String> properties = propertyManager.listProperties();
//			List<String> children = ZkUtils.getChildren(zkSpliceDerbyPropertyPath, false);
			for (String property: properties) {
				String value = propertyManager.getProperty(property);
				service.setProperty(property, value);
			}
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "getServiceProperties Failed", Exceptions.parseException(e));
		}
		SpliceLogUtils.trace(LOG,"getServiceProperties serviceName: %s, defaultProperties %s",serviceName, defaultProperties);
//		Properties service = new Properties(SpliceUtils.getAllProperties(defaultProperties));

		service.setProperty(Property.SERVICE_PROTOCOL,"com.splicemachine.db.database.Database");
		service.setProperty(EngineType.PROPERTY,Integer.toString(getEngineType()));
		//service.setProperty(DataDictionary.CORE_DATA_DICTIONARY_VERSION,"10.9");
//		service.setProperty(Property.REQUIRE_AUTHENTICATION_PARAMETER, "true");
//		service.setProperty("derby.language.logQueryPlan", "true"); // unclear of this...
		if (LOG.isTraceEnabled()) {
			LOG.trace("getServiceProperties actual properties serviceName" + serviceName + ", properties " + service);
		}
		return service;
	}

	public void saveServiceProperties(String serviceName, StorageFactory storageFactory, Properties properties, boolean replace) throws StandardException {
		SpliceLogUtils.trace(LOG,"saveServiceProperties with storageFactory serviceName: %s, properties %s, replace %s",serviceName, properties, replace);
		for (Object key :properties.keySet()) {
			if (!propertyManager.propertyExists((String)key)) {
				propertyManager.addProperty((String)key,properties.getProperty((String)key));
			}
		}
	}

    public void saveServiceProperties(String serviceName,Properties properties) throws StandardException {
		SpliceLogUtils.trace(LOG,"saveServiceProperties serviceName: %s, properties %s",serviceName, properties);
        PropertyManager pm = EngineDriver.driver().propertyManager();
		for (Object key :properties.keySet()) {
			if (!pm.propertyExists((String)key)) {
				pm.addProperty((String)key,properties.getProperty((String)key));
			}
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

