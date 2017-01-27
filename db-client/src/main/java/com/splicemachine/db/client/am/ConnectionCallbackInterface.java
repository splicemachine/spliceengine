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
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

// Methods implemented by the common Connection class to handle
// certain events that may originate from the material or common layers.
//
// Reply implementations may update connection state via this interface.

public interface ConnectionCallbackInterface {
    public void completeLocalCommit();

    public void completeLocalRollback();

    public void completeAbnormalUnitOfWork();

    public void completeChainBreakingDisconnect();

    public void completeSqlca(Sqlca e);
    
    /**
     *
     * Rollback the UnitOfWorkListener specifically.
     * @param uwl The UnitOfWorkListener to be rolled back.
     *
     */
    public void completeAbnormalUnitOfWork(UnitOfWorkListener uwl);

    /**
     * Completes piggy-backing of the new current isolation level by
     * updating the cached copy in am.Connection.
     * @param pbIsolation new isolation level from the server
     */
    public void completePiggyBackIsolation(int pbIsolation);

    /**
     * Completes piggy-backing of the new current schema by updating
     * the cached copy in am.Connection.
     * @param pbSchema new current schema from the server
     */
    public void completePiggyBackSchema(String pbSchema);
}
