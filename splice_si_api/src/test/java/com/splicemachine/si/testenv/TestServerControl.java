package com.splicemachine.si.testenv;

import com.splicemachine.access.api.ServerControl;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/26/16
 */
public class TestServerControl implements ServerControl{
    @Override public void startOperation() throws IOException{ }
    @Override public void stopOperation() throws IOException{ }
    @Override public void ensureNetworkOpen() throws IOException{ }
}
