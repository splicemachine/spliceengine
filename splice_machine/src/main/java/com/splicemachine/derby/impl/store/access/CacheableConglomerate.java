/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
