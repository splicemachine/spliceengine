/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 */

package org.apache.ddlutils.model;

import java.util.ArrayList;
import java.util.List;

/**
 * List of Grantee, Grantor pairs shared by a Resource.
 */
public abstract class Grantable {

    private List<GrantPair> grantPairs = new ArrayList<>();

    public List<GrantPair> getGrantPairs() {
        return grantPairs;
    }

    public void addGrantPair(String grantee, String grantor) {
        grantPairs.add(new GrantPair(grantee, grantor));
    }

    public static class GrantPair {
        public final String grantee;
        public final String grantor;

        private GrantPair(String grantee, String grantor) {
            this.grantee = grantee;
            this.grantor = grantor;
        }
    }

}
