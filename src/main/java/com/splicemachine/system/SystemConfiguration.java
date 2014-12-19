package com.splicemachine.system;

import java.io.IOException;

/**
 * Overall representation of a System Configuration.
 *
 * @author Scott Fines
 *         Date: 1/13/15
 */
public interface SystemConfiguration {

    /**
     * Generate a well-formatted string which contains the data important to this configuration.
     * @return a well-formatted string containing important data about this configuration
     */
    String prettyPrint() throws IOException;
}
