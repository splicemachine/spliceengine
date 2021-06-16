/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

import java.util.*;

import com.splicemachine.db.iapi.services.classfile.ClassHolder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author Jeff Cunningham
 *         Date: 1/31/14
 */
public class SpliceLogManager implements Logging {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getRootLogger();
    private static final List<String> LOG4JLEVELS =
            Arrays.asList("ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF", "TRACE");

    @Override
    public List<String> getLoggerNames() {
        List<String> loggerNames = new ArrayList<String>();
        LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        Collection<org.apache.logging.log4j.core.Logger> loggers = ctx.getLoggers();

        for (Iterator<org.apache.logging.log4j.core.Logger> iterator = loggers.iterator(); iterator.hasNext();) {
            String loggerName = iterator.next().getName();
            if (loggerName.startsWith("com.splicemachine")) {
                loggerNames.add(loggerName);
            }
        }
        Collections.sort(loggerNames);
        return loggerNames;
    }

    @Override
    public List<String> getAvailableLevels() {
        return LOG4JLEVELS;
    }

    @Override
    public String getLoggerLevel(String loggerName) {

        Logger logger = LogManager.getLogger(loggerName);
        if (logger == null) {
            throw new IllegalArgumentException("Logger \"" + loggerName +
                    "\" does not exist");
        }
        Level level = logger.getLevel();
        if (level == null) {
            return "";
        }
        return level.toString();
    }

    @Override
    public void setLoggerLevel(String loggerName, String levelName) {
        String prospectiveLevel = null;
        if (levelName != null) {
            prospectiveLevel = levelName.trim();
        }
        if (prospectiveLevel == null ||
                prospectiveLevel.isEmpty() ||
                ! LOG4JLEVELS.contains(prospectiveLevel.toUpperCase())) {
            throw new IllegalArgumentException("Log level \"" + levelName +
                    "\" is not valid.");
        }
        Logger logger = LogManager.getLogger(loggerName);
        if (logger == null) {
            throw new IllegalArgumentException("Logger \"" + loggerName +
                    "\" does not exist");
        }
        Level newLevel = Level.toLevel(levelName);
        Configurator.setLevel(loggerName, newLevel);
    }
}
