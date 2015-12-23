package com.splicemachine.storage;

import com.splicemachine.access.api.ServerControl;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MServerControl implements ServerControl{
    public static final ServerControl INSTANCE = new MServerControl();

    private MServerControl(){}
    @Override public void startOperation() throws IOException{ }
    @Override public void stopOperation() throws IOException{ }
    @Override public void ensureNetworkOpen() throws IOException{ }
}
