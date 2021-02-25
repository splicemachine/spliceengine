/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.depend.Provider;

import java.util.Arrays;
import java.util.Map;

import static com.splicemachine.db.iapi.reference.Property.SPLICE_TIMESTAMP_FORMAT;

/**
 * This class represents a system property that is used internally in an SPS (Stored Procedure Statement), it is used
 * to model an SPS dependency on a property such that we can invalidate it when the property is changed by the user.
 * For example, if the user defines a timestamp format property and we have an SPS the contains a timestamp column we
 * create an SPSProperty for the timestamp property and add the SPS as Dependent on it, later on, when the user modifies
 * the timestamp format, we use the DependencyManager to invalidate all SPS dependents on that property.
 */
public class SPSProperty implements Provider {

    public enum Property {
        TimestampFormat(SPLICE_TIMESTAMP_FORMAT)
        ;

        private String userPropertyName;
        Property(String name) {
            userPropertyName = name;
        }
        public String getUserProperty() {
            return userPropertyName;
        }
    }

    final UUID uuid;
    final String name;

    private SPSProperty(UUID uuid, String name) {
        this.uuid = uuid;
        this.name = name;
    }

    @Override
    public DependableFinder getDependableFinder() {
        return null;
    }

    @Override
    public String getObjectName() {
        return name;
    }

    @Override
    public UUID getObjectID() {
        return uuid;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public String getClassType() {
        return SPS_PROPERTY;
    }

    private static Map<Property, SPSProperty> propertyMap;

    public static SPSProperty getSPSPropertyFor(Property property) {
        if(propertyMap.containsKey(property)) {
            return propertyMap.get(property);
        }
        ModuleFactory moduleFactory = Monitor.getMonitor();
        SanityManager.ASSERT(moduleFactory != null);
        assert moduleFactory != null; // make Intellij happy
        UUIDFactory uuidFactory = moduleFactory.getUUIDFactory();
        SanityManager.ASSERT(uuidFactory != null);
        assert uuidFactory != null; // make Intellij happy
        UUID uuid = uuidFactory.createUUID();
        SPSProperty result = new SPSProperty(uuid, property.getUserProperty());
        propertyMap.put(property, result);
        return result;
    }

    public static boolean isSpsProperty(String name) {
        return Arrays.stream(Property.values()).anyMatch(p -> p.userPropertyName.equals(name));
    }

    public static SPSProperty getSPSPropertyFor(String propertyName) {
        if(!isSpsProperty(propertyName)) {
            return null;
        }
        Property p = Arrays.stream(Property.values()).filter(p -> p.userPropertyName.equals(propertyName)).findFirst().orElse(null);
        assert p != null;
        return getSPSPropertyFor(p);
    }
}
