package com.splicemachine.utils.logging;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaskPatternLayout extends PatternLayout {

    private String maskString = "***MASKED SENSITIVE INFO***";
    private Pattern maskPattern = null;
    private Logger logger = Logger.getLogger(this.getClass());

    public String getMaskPattern() {
        return maskPattern.toString();
    }

    public void setMaskPattern(String pattern) {
        try {
            maskPattern = Pattern.compile(pattern);
        } catch (Exception e) {
            logger.error("Error to compile regex for mask pattern, will not mask anything", e);
        }
    }

    public void setMaskString(String maskString) {
        this.maskString = maskString;
    }

    public String getMaskString() {
        return this.maskString;
    }

    public static String maskMessage(String message, Pattern maskPattern, String maskString) {
        Matcher matcher = maskPattern.matcher(message);

        if (matcher.find()) {
            StringBuilder maskedMessage = new StringBuilder("");
            for (int i = 0; i <= matcher.groupCount(); i++) {
                int start;
                int end;
                if (i == 0) {
                    start = 0;
                } else {
                    start = matcher.end(i);
                    maskedMessage.append(maskString);
                }
                if (i == matcher.groupCount()) {
                    end = message.length() - 1;
                } else {
                    end = matcher.start(i + 1) - 1;
                }
                if (start > end || end >= message.length()) {
                    continue;
                }
                for (int j = start; j <= end; j++) {
                    maskedMessage.append(message.charAt(j));
                }
            }
            return String.valueOf(maskedMessage);
        }
        return message;
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
