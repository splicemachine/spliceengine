package com.splicemachine.db.impl.sql.conn;

import com.splicemachine.db.iapi.services.cache.Cacheable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.impl.sql.GenericStatement;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;

/**
 */
public class CachedStatement implements Cacheable {

    private GenericStorablePreparedStatement ps;
    private Object identity;

    public CachedStatement() {
    }

    /**
     * Get the PreparedStatement that is associated with this Cacheable
     */
    public GenericStorablePreparedStatement getPreparedStatement() {
        return ps;
    }

    @Override
    public void clean(boolean forRemove) {
    }

    @Override
    public Cacheable setIdentity(Object key) {

        identity = key;
        ps = new GenericStorablePreparedStatement((GenericStatement) key);
        ps.setCacheHolder(this);

        return this;
    }

    @Override
    public Cacheable createIdentity(Object key, Object createParameter) {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not expecting any create() calls");

        return null;

    }

    @Override
    public void clearIdentity() {

        if (SanityManager.DEBUG)
            SanityManager.DEBUG("StatementCacheInfo", "CLEARING IDENTITY: " + ps.getSource());
        ps.setCacheHolder(null);

        identity = null;
        ps = null;
    }

    @Override
    public Object getIdentity() {
        return identity;
    }

    @Override
    public boolean isDirty() {
        return false;
    }

}
