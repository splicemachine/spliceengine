/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

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
