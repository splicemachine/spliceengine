/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
        defaultsList.add(new OlapConfigurations());
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
