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

package com.splicemachine.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.splicemachine.access.configuration.ConfigurationDefault;
import com.splicemachine.access.configuration.ConfigurationDefaultsList;

/**
 * Test with empty config defaults. Tests can add their own defaults.
 */
public class EmptyConfigurationDefaultsList implements ConfigurationDefaultsList {
    private final List<ConfigurationDefault> defaultsList = new ArrayList<>();

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
