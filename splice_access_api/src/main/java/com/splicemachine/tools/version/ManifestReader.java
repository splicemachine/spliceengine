/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.tools.version;

import com.splicemachine.access.api.DatabaseVersion;

import javax.management.*;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Class that reads Splice build info from splice_machine-*.jar MANIFEST and registers with JMX.<br/>
 * Supports finding Splice build info from JMX and Splice Admin procedure.
 *
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
public class ManifestReader {
    public static final String SPLICEMACHINE_VERSION = "com.splicemachine.version:type=DatabaseVersion";

    private ManifestFinder manifestFinder;

    public ManifestReader() {
        this(new ManifestFinder());
    }

    public ManifestReader(ManifestFinder manifestFinder) {
        this.manifestFinder = manifestFinder;
    }

    /**
     * Register with JMX
     */
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        try{
            mbs.registerMBean(createVersion(),new ObjectName(SPLICEMACHINE_VERSION));
        }catch(InstanceAlreadyExistsException ignored){
            /*
             * For most purposes, this should never happen. However, it's possible to happen
             * when you are booting a regionserver and master in the same JVM (e.g. for testing purposes); Since
             * we can only really have one version of the software on a single node at one time, we just ignore
             * this exception and don't worry about it too much.
             */
        }
    }

    /**
     * Create a Splice Machine version object from the contents of the manifest file.
     *
     * @return version of the Splice Machine software from the manifest file
     */
    public DatabaseVersion createVersion() {
        return new SimpleDatabaseVersion(getManifestProps());
    }

    private Map<String, String> getManifestProps() {
        Manifest spliceManifest = manifestFinder.findManifest();
        return toMap(spliceManifest);
    }

    private static Map<String, String> toMap(Manifest manifest) {
        Map<String, String> rawMap = new HashMap<>();
        if(manifest == null) return rawMap;
        Attributes mainAttributes = manifest.getMainAttributes();
        if(mainAttributes==null) return rawMap;
        for (Map.Entry<Object, Object> entry : mainAttributes.entrySet()) {
            rawMap.put(entry.getKey().toString(), (String) entry.getValue());
        }
        return rawMap;
    }

}
