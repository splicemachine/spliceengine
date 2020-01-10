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
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
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

/**
 * Java 2 PrivilegedAction encapsulation of attempting to acquire driver-general properties as a System resource.
 */
public class GetResourceInputStreamAction implements java.security.PrivilegedAction {
    // Name for loading the resource.
    private String resourceName_ = null;
    // Path of the resource being loaded.
    private String resourcePath_ = null;
    // Class loader being used to load the resource.
    private String resourceLoaderId_ = null;

    //-------------------- Constructors --------------------

    public GetResourceInputStreamAction(String resourceName) {
        resourceName_ = resourceName;
    }

    //-------------------- methods --------------------

    public Object run() {
        try {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            if (contextLoader != null) {
                java.net.URL resourceUrl = contextLoader.getResource(resourceName_);
                if (resourceUrl != null) {
                    resourcePath_ = resourceUrl.getPath();
                    resourceLoaderId_ = "Context ClassLoader: " + contextLoader;
                    return contextLoader.getResourceAsStream(resourceName_);
                }
            }
            ClassLoader thisLoader = getClass().getClassLoader();
            if (thisLoader != contextLoader) {
                java.net.URL resourceUrl = thisLoader.getResource(resourceName_);
                if (resourceUrl != null) {
                    resourcePath_ = resourceUrl.getPath();
                    resourceLoaderId_ = "Driver ClassLoader: " + thisLoader;
                    return thisLoader.getResourceAsStream(resourceName_);
                }
            }
            ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
            if (systemLoader != contextLoader &&
                    systemLoader != thisLoader) {
                java.net.URL resourceUrl = systemLoader.getResource(resourceName_);
                if (resourceUrl != null) {
                    resourcePath_ = resourceUrl.getPath();
                    resourceLoaderId_ = "System ClassLoader: " + systemLoader;
                    return systemLoader.getResourceAsStream(resourceName_);
                }
            }
            return null;
        } catch (java.security.AccessControlException ace) {
            // This happens in an Applet environment,
            // so return with null.
            return null;
        }
    }

    public void setResourceName(String resourceName) {
        resourceName_ = resourceName;
    }

    public String getResourcePath() {
        return resourcePath_;
    }

    public String getResourceLoaderId() {
        return resourceLoaderId_;
    }

}
