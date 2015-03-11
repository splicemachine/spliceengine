package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.Cacheable;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;


class CacheableConglomerate implements Cacheable {
    private Long            conglomid;
    private Conglomerate    conglom;

    CacheableConglomerate() {
    }

    protected Conglomerate getConglom() {
        return(this.conglom);
    }

	public Cacheable setIdentity(Object key) throws StandardException {
        return(null);
    }

	public Cacheable createIdentity(Object key, Object createParameter)  throws StandardException {
        this.conglomid = (Long) key;
        this.conglom   = ((Conglomerate) createParameter);
        return(this);
    }

	public void clearIdentity() {
        this.conglomid = null;
        this.conglom   = null;
    }

	public Object getIdentity() {
        return(this.conglomid);
    }


	public boolean isDirty() {
        return(false);
    }

	public void clean(boolean forRemove) throws StandardException {
    }
}
