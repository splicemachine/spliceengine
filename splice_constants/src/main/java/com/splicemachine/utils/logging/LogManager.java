package com.splicemachine.utils.logging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Jeff Cunningham
 *         Date: 1/31/14
 */
public class LogManager implements Logging {

    private static final Logger LOGGER = Logger.getRootLogger();
    private static final List<String> LOG4JLEVELS =
            Arrays.asList("ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF", "TRACE");

    @Override
    public List<String> getLoggerNames() {
        List<String> loggerNames = new ArrayList<>();
        Enumeration loggers = LOGGER.getLoggerRepository().getCurrentLoggers();
        while (loggers.hasMoreElements()) {
            String loggerName = ((Logger) loggers.nextElement()).getName();
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

        Logger logger = Logger.getLogger(loggerName);
        if (logger == null) {
            throw new IllegalArgumentException("Logger \"" + loggerName +
                    "\" does not exist");
        }
        Level level = logger.getEffectiveLevel();
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
        Logger logger = Logger.getLogger(loggerName);
        if (logger == null) {
            throw new IllegalArgumentException("Logger \"" + loggerName +
                    "\" does not exist");
        }
        Level newLevel = Level.toLevel(levelName);
        logger.setLevel(newLevel);
    }
}
