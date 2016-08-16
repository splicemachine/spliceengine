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
 *
 */

package com.splicemachine.db.client.cluster;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Scott Fines
 *         Date: 8/15/16
 */
public enum ConnectionStrategy implements ConnectionSelectionStrategy{
    RANDOM{
        @Override
        public int nextServer(int previous,int numServers){
            return ThreadLocalRandom.current().nextInt(0,numServers);
        }
    },
    ROUND_ROBIN{
        @Override
        public int nextServer(int previous,int numServers){
            return (previous+1) % numServers;
        }
    },
}
