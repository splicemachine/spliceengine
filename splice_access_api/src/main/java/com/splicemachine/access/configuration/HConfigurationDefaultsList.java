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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The ConfigurationDefaultsList implementation used by Splice configuration.
 * <p/>
 * Tests can create their own or subclass to add their specific defaults.
 */
public class HConfigurationDefaultsList implements ConfigurationDefaultsList {
    private final List<ConfigurationDefault> defaultsList = new ArrayList<>();

    public HConfigurationDefaultsList() {
        defaultsList.add(new SIConfigurations());
        defaultsList.add(new HBaseConfiguration());
        defaultsList.add(new PipelineConfiguration());
        defaultsList.add(new SQLConfiguration());
        defaultsList.add(new StatsConfiguration());
        defaultsList.add(new StorageConfiguration());
        defaultsList.add(new DDLConfiguration());
        defaultsList.add(new OperationConfiguration());
        defaultsList.add(new AuthenticationConfiguration());
    }

    @Override
    public Iterator<ConfigurationDefault> iterator() {
        return defaultsList.iterator();
    }

    @Override
    public ConfigurationDefaultsList addConfig(ConfigurationDefault configurationDefault) {
        defaultsList.add(configurationDefault);
        return this;
    }
}
