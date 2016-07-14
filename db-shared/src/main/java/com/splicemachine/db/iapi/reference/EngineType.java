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

package com.splicemachine.db.iapi.reference;

/**
 * Derby engine types. Enumerate different modes the
 * emmbedded engine (JDBC driver, SQL langauge layer and
 * store) can run in. A module can query the monitor to
 * see what type of service is being requested in terms
 * of its engine type and then use that in a decision
 * as to if it is suitable.
 * 
 * @see com.splicemachine.db.iapi.services.monitor.ModuleSupportable
 * @see com.splicemachine.db.iapi.services.monitor.Monitor#isDesiredType(Properties, int)
 * @see com.splicemachine.db.iapi.services.monitor.Monitor#getEngineType(Properties)
 *
 */
public interface EngineType {
    /**
     * Full database engine, the typical configuration.
     */
    int STANDALONE_DB = 0x00000002;
    
    /**
     * Property used to define the type of engine required.
     * If not set defaults to STANDALONE_DB.
     */
    String PROPERTY = "derby.engineType";
}