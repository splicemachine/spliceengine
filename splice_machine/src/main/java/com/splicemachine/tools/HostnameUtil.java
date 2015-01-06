package com.splicemachine.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostnameUtil {

    /**
     * Returns a hostname.  On linux this will probably return the value from /etc/hostname but
     * the Java API doesn't seem to guarantee this. Thus probably should use this with caution,
     * for non critical logging, etc.
     */
    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
    }

}
