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

package com.splicemachine.access.configuration;

/**
 * List of subsystem defaults that will be used, in order, to configure Splice.
 * <p/>
 * The order in which {@link ConfigurationDefault}s are added is important. Although
 * unlikely, defaults added later can override defaults of same name.
 */
public interface ConfigurationDefaultsList extends Iterable<ConfigurationDefault> {

    /**
     * Add a configuration default to this list. <b>Order is important</b>.
     * @param configurationDefault the subsystem default to add.
     * @return the instance of this list to which another default can be added.
     */
    ConfigurationDefaultsList addConfig(ConfigurationDefault configurationDefault);
}

