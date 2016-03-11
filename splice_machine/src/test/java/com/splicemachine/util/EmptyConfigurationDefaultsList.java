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
