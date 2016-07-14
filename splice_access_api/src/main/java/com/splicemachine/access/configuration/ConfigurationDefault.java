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
 * Common Configuration interface for Splice subsystems.  Subsystems will implement this interface
 * to set default values for configuration properties provided the configuration source does not
 * already have a value.
 * <p/>
 * All instances of this interface are collected in an instance of {@link ConfigurationDefaultsList}
 * for the builder to collect all configuration properties.
 * See {@link #setDefaults(ConfigurationBuilder, ConfigurationSource)} as to how properties get set on
 * the {@link ConfigurationBuilder}.
 * @see ConfigurationBuilder
 * @see SConfigurationImpl
 */
public interface ConfigurationDefault {

    /**
     * Called by {@link ConfigurationBuilder#build(ConfigurationDefaultsList, ConfigurationSource)}
     * to give instances of this interface the opportunity to affect the configuration by setting
     * their default values for configuration properties.  These default values are only set if the
     * {@link ConfigurationSource} does not already have a value for the given property.  Otherwise,
     * we'll get the value that was previously set on the configuration source.
     * <p/>
     * Example:
     * <pre>
     *  builder.ddlRefreshInterval = configurationSource.getLong(DDL_REFRESH_INTERVAL, DEFAULT_DDL_REFRESH_INTERVAL);
     * </pre>
     * @param builder the configuration builder that's collecting the configuration values.
     * @param configurationSource the source of the configuration which may already
     */
    void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource);
}
