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
