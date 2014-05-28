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
import org.apache.log4j.Logger;

import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
public class ManifestReader {
    private static final Logger LOG = Logger.getLogger(ManifestReader.class);

    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException,
        NotCompliantMBeanException,
        InstanceAlreadyExistsException,
        MBeanRegistrationException {
        ObjectName name = new ObjectName(JMXUtils.SPLICEMACHINE_VERSION);
        mbs.registerMBean(new SpliceMachineVersionImpl(readManifestOnClasspath()),name);
    }

    public static class SpliceMachineVersionImpl implements SpliceMachineVersion {
        private final Map<String,String> versionInfo;

        public SpliceMachineVersionImpl(Map<String,String> manifestProps) {
            if (manifestProps == null) {
                SpliceLogUtils.warn(LOG, "Unable to register Splice Machine Version JMX entry. Version information " +
                    "will be unavailable.");
            }
            this.versionInfo = manifestProps;
        }

        @Override
        public String getRelease() {
            return this.versionInfo.get("Release");
        }

        @Override
        public String getImplementationVersion() {
            return this.versionInfo.get("Implementation-Version");
        }

        @Override
        public String getBuildTime() {
            return this.versionInfo.get("Build-Time");
        }

        @Override
        public String getURL() {
            return this.versionInfo.get("URL");
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("Release ").append(getRelease()).append('\n').append("ImplementationVersion ").
                append(getImplementationVersion()).append('\n').append("BuildTime ").append(getBuildTime()).append('\n').
                   append("URL ").append(getURL()).append('\n');
            return buf.toString();
        }
    }

    protected Map<String,String> readManifestOnClasspath() {
        Map<String,String> map = null;
        try {
            Enumeration resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (resEnum.hasMoreElements()) {
                URL url = (URL) resEnum.nextElement();
                if (url.getFile().contains("splice_machine-")) {
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
            SpliceLogUtils.warn(LOG, "Unable to register Splice Machine Version JMX entry. Version information will be unavailable.", e);
        }
        if (map == null) {
            SpliceLogUtils.warn(LOG, "Didn't find splice machine jar on the classpath. Version information will be unavailable.");
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

    // for testing
    SpliceMachineVersion create() {
        return new SpliceMachineVersionImpl(readManifestOnClasspath());
    }

    private static Map<String,String> translateMap(Map<String,String> fromMap) {
        Map<String,String> manifestToSpliceAttrMap = Maps.newLinkedHashMap();
        // Add entries in the order we want to see them
        manifestToSpliceAttrMap.put("Release Version",fromMap.get("Release"));
        manifestToSpliceAttrMap.put("Implementation Version",fromMap.get("Implementation-Version"));
        manifestToSpliceAttrMap.put("Build Date",fromMap.get("Build-Time"));
        manifestToSpliceAttrMap.put("URL",fromMap.get("URL"));
        return manifestToSpliceAttrMap;
    }

}
