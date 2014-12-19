package com.splicemachine.tools.version;

import com.splicemachine.hbase.jmx.JMXUtils;

import javax.management.*;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Manifest;

/**
 * Class that reads Splice build info from splice_machine-*.jar MANIFEST and registers with JMX.<br/>
 * Supports finding Splice build info from JMX and Splice Admin procedure.
 *
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
public class ManifestReader {

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
        mbs.registerMBean(createVersion(), new ObjectName(JMXUtils.SPLICEMACHINE_VERSION));
    }

    /**
     * Create a Splice Machine version object from the contents of the manifest file.
     *
     * @return version of the Splice Machine software from the manifest file
     */
    public SpliceMachineVersion createVersion() {
        return new SimpleSpliceMachineVersion(getManifestProps());
    }

    private Map<String, String> getManifestProps() {
        Manifest spliceManifest = manifestFinder.findManifest();
        return toMap(spliceManifest);
    }

    private static Map<String, String> toMap(Manifest manifest) {
        Map<String, String> rawMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : manifest.getMainAttributes().entrySet()) {
            rawMap.put(entry.getKey().toString(), (String) entry.getValue());
        }
        return rawMap;
    }

}
