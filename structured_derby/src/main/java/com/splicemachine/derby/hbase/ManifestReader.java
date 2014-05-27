package com.splicemachine.derby.hbase;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import com.google.common.collect.Maps;

import com.splicemachine.hbase.jmx.JMXUtils;

/**
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
public class ManifestReader {
    private SpliceMachineVersion version;

    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException,
        NotCompliantMBeanException,
        InstanceAlreadyExistsException,
        MBeanRegistrationException {
        ObjectName name = new ObjectName(JMXUtils.SPLICEMACHINE_VERSION);
        version = new SpliceMachineVersionImpl(readManifestOnClasspath());
        mbs.registerMBean(version,name);
    }

    public static class SpliceMachineVersionImpl implements SpliceMachineVersion {
        private final Map<String,String> versionInfo;

        public SpliceMachineVersionImpl(Map<String,String> versionInfo) {
            this.versionInfo = translateMap(versionInfo);
        }

        @Override
        public String getVersionInfo() {
            // TODO: Is this what we want returned?
            return versionInfo.toString();
        }

        public String toString() {
            return getVersionInfo();
        }
    }


    protected Map<String,String> readManifestOnClasspath() {
        Map<String,String> map = null;
        try {
            Enumeration resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (resEnum.hasMoreElements()) {
                URL url = (URL) resEnum.nextElement();
                if (url.getFile().startsWith("splice_machine-")) {
                    InputStream is = null;
                    try {
                        is = url.openStream();
                        if (is != null) {
                            Manifest manifest = new Manifest(is);
                            map = toMap(manifest);
                            break;
                        }
                    } finally {
                        if (is != null) {
                            is.close();
                        }
                    }
                }
            }
        } catch (IOException e) {
            // TODO: log
        }
        return map;
    }

    protected static Map<String,String> toMap(Manifest manifest) {
        Map<String,String> rawMap = new HashMap<String, String>();

        for (Map.Entry<Object,Object> entry : manifest.getMainAttributes().entrySet()) {
            rawMap.put(entry.getKey().toString(), (String) entry.getValue());
        }
        return rawMap;
    }

    protected static Map<String,String> translateMap(Map<String,String> fromMap) {
        Map<String,String> manifestToSpliceAttrMap = Maps.newLinkedHashMap();
        // adding entries in the order we want to see them
        manifestToSpliceAttrMap.put("Release Version",fromMap.get("Release"));
        manifestToSpliceAttrMap.put("Implementation Version",fromMap.get("Implementation-Version"));
        manifestToSpliceAttrMap.put("Build Date",fromMap.get("Build-Time"));
        return manifestToSpliceAttrMap;
    }

}
