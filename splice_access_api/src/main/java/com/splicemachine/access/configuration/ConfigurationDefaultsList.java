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

