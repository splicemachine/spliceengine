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
