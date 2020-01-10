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
