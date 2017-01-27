/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
