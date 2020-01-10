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

package com.splicemachine.utils.logging;

import com.splicemachine.annotations.Description;
import com.splicemachine.annotations.PName;
import javax.management.MXBean;

/**
 * JMX Adaptor for Log4J logging control.  It's made to resemble the same
 * control for java.util.logging.Logging.
 *
 * @author Jeff Cunningham
 *         Date: 1/31/14
 */
@MXBean
public interface Logging {
    /**
     * Returns the list of currently registered loggers.
     *
     * @return A list of <tt>String</tt> each of which is a
     *         currently registered <tt>Logger</tt> name.
     */
    @Description(value="Get the list of SpliceMachine logger names.")
    java.util.List<String> getLoggerNames();

    /**
     * Returns the list of available log levels.
     * @return A list of <tt>String</tt> each of which is an
     *          available log level.
     */
    @Description(value="Get the list of available logging levels.")
    java.util.List<String> getAvailableLevels();

    /**
     * Gets the name of the log level associated with the specified logger.
     * If the specified logger does not exist, <tt>null</tt>
     * is returned.
     * This method first finds the logger of the given name and
     * then returns the name of the log level by calling:
     * <blockquote>
     *   {@link java.util.logging.Logger#getLevel Logger.getLevel()}.{@link java.util.logging.Level#getName getName()};
     * </blockquote>
     *
     * <p>
     * If the <tt>Level</tt> of the specified logger is <tt>null</tt>,
     * which means that this logger's effective level is inherited
     * from its parent, an empty string will be returned.
     *
     * @param loggerName The name of the <tt>Logger</tt> to be retrieved.
     *
     * @return The name of the log level of the specified logger; or
     *         an empty string if the log level of the specified logger
     *         is <tt>null</tt>.  If the specified logger does not
     *         exist, <tt>null</tt> is returned.
     *
     * @see java.util.logging.Logger#getLevel
     */
    @Description(value="Get the current logging level for the given logger.")
    String getLoggerLevel(@PName("loggerName") String loggerName);

    /**
     * Sets the specified logger to the specified new level.
     * If the <tt>levelName</tt> is not <tt>null</tt>, the level
     * of the specified logger is set to the parsed <tt>Level</tt>
     * matching the <tt>levelName</tt>.
     * If the <tt>levelName</tt> is <tt>null</tt>, the level
     * of the specified logger is set to <tt>null</tt> and
     * the effective level of the logger is inherited from
     * its nearest ancestor with a specific (non-null) level value.
     *
     * @param loggerName The name of the <tt>Logger</tt> to be set.
     *                   Must be non-null.
     * @param levelName The name of the level to set the specified logger to,
     *                 or <tt>null</tt> if to set the level to inherit
     *                 from its nearest ancestor.
     *
     * @throws IllegalArgumentException if the specified logger
     * does not exist, or <tt>levelName</tt> is not a valid level name.
     *
     * @throws SecurityException if a security manager exists and if
     * the caller does not have LoggingPermission("control").
     *
     * @see java.util.logging.Logger#setLevel
     */
    @Description(value="Set the logging level for the given logger.")
    void setLoggerLevel(@PName("loggerName") String loggerName, @PName("levelName") String levelName);

}
