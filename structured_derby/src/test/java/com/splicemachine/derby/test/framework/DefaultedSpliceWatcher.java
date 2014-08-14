package com.splicemachine.derby.test.framework;

public class DefaultedSpliceWatcher extends SpliceWatcher {

    // TODO: we can get rid of this class now, use the same constructor in SpliceWatcher directly.
    public DefaultedSpliceWatcher(String schema) {
        super(schema);
    }

}
