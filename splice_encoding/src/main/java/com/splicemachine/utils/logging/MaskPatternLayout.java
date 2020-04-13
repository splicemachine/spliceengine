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

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import com.splicemachine.utils.StringUtils;

import java.util.regex.Pattern;

public class MaskPatternLayout extends PatternLayout {

    private String maskString = "********";
    private Pattern maskPattern = null;
    private Logger logger = Logger.getLogger(this.getClass());

    public String getMaskPattern() {
        return maskPattern.toString();
    }

    public void setMaskPattern(String pattern) {
        try {
            maskPattern = Pattern.compile(pattern);
        } catch (Exception e) {
            logger.error("Error to compile regex for mask pattern, will not mask anything. Invalid pattern: "
                    + pattern, e);
        }
    }

    public void setMaskString(String maskString) {
        this.maskString = maskString;
    }

    public String getMaskString() {
        return this.maskString;
    }

    public static String maskMessage(String message, Pattern maskPattern, String maskString) {
        return StringUtils.maskMessage(message,maskPattern,maskString);
    }

    @Override
    public String format(LoggingEvent event) {
        if (maskPattern == null || !(event.getMessage() instanceof String)) {
            return super.format(event);
        }

        String message = event.getRenderedMessage();
        String maskedMessage = maskMessage(message, maskPattern, maskString);
        Throwable throwable = event.getThrowableInformation() != null ?
                event.getThrowableInformation().getThrowable() : null;
        LoggingEvent maskedEvent = new LoggingEvent(event.fqnOfCategoryClass,
                    Logger.getLogger(event.getLoggerName()), event.timeStamp,
                    event.getLevel(), maskedMessage, throwable);
        return super.format(maskedEvent);
    }



}
