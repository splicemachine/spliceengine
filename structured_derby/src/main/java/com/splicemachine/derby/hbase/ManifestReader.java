package com.splicemachine.derby.hbase;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
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

import org.apache.log4j.Logger;

import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Class that reads Splice build info from splice_machine-*.jar MANIFEST and registers with JMX.<br/>
 * Supports finding Splice build info from JMX and Splice Admin procedure.
 *
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
public class ManifestReader {
    private static final Logger LOG = Logger.getLogger(ManifestReader.class);

    /**
     * Register with JMX
     * @param mbs Managed Bean Server
     * @throws MalformedObjectNameException
     * @throws NotCompliantMBeanException
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     */
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException,
        NotCompliantMBeanException,
        InstanceAlreadyExistsException,
        MBeanRegistrationException {
        ObjectName name = new ObjectName(JMXUtils.SPLICEMACHINE_VERSION);
        mbs.registerMBean(new SpliceMachineVersionImpl(readManifestOnClasspath()),name);
    }

    /**
     * Interface to expose to JMX for version info
     */
    @MXBean
    public interface SpliceMachineVersion {
        /** @return the Splice release version */
        String getRelease();
        /** @return the Git commit that was used to build this version */
        String getImplementationVersion();
        /** @return the date/time of the build */
        String getBuildTime();
        /** @return the Splice Machine URL */
        String getURL();
    }

    /**
     * Class that implements interface exposed to JMX
     */
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
        public String getRelease() {return (this.versionInfo == null ? "Unknown" : this.versionInfo.get("Release"));}

        @Override
        public String getImplementationVersion() {return (this.versionInfo == null ? "Unknown" : this.versionInfo.get("Implementation-Version"));}

        @Override
        public String getBuildTime() {return (this.versionInfo == null ? "Unknown" : this.versionInfo.get("Build-Time"));}

        @Override
        public String getURL() {return (this.versionInfo == null ? "Unknown" : this.versionInfo.get("URL"));}

        @Override
        public String toString() {
            return "Release " + getRelease() + '\n' + "ImplementationVersion " + getImplementationVersion() + '\n' +
                "BuildTime " + getBuildTime() + '\n' + "URL " + getURL() + '\n';
        }
    }

    /**
     * Read the manifest from the splice_machine-*.jar
     * @return manifest entries mapped to strings.
     */
    protected Map<String,String> readManifestOnClasspath() {
        Map<String,String> map = null;
        try {
            Enumeration resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (resEnum.hasMoreElements()) {
                URL url = (URL) resEnum.nextElement();
                if (url.getFile().contains("/splice_machine-")) {
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

    /**
     * Map manifest entries name/value to strings
     * @param manifest from this manifest
     * @return map &lt;entryName, entryValue&gt;
     */
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

}
