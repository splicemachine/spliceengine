package com.splicemachine.utils.logging;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaskPatternLayout extends PatternLayout {

    private static final String MASK = "***MASKED SENSITIVE INFO***";
    private Pattern maskPattern = null;

    public String getMaskPattern() {
        return maskPattern.toString();
    }

    public void setMaskPattern(String pattern) {
        maskPattern = Pattern.compile(pattern);
    }

    @Override
    public String format(LoggingEvent event) {
        if (maskPattern == null || !(event.getMessage() instanceof String)) {
            return super.format(event);
        }

        String message = event.getRenderedMessage();
        Matcher matcher = maskPattern.matcher(message);

        if (matcher.find()) {
            String maskedMessage = "";
            for(int i = 0; i <= matcher.groupCount(); i++) {
                int start;
                int end;
                if (i == 0) {
                    start = 0;
                } else {
                    start = matcher.end(i);
                    maskedMessage += MASK;
                }
                if (i == matcher.groupCount()) {
                    end = message.length()-1;
                } else {
                    end = matcher.start(i+1) - 1;
                }
                if (start > end || end >= message.length()) {
                    continue;
                }
                for (int j = start; j <= end; j++) {
                    maskedMessage += message.charAt(j);
                }
            }
            Throwable throwable = event.getThrowableInformation() != null ?
                event.getThrowableInformation().getThrowable() : null;
            LoggingEvent maskedEvent = new LoggingEvent(event.fqnOfCategoryClass,
                    Logger.getLogger(event.getLoggerName()), event.timeStamp,
                    event.getLevel(), maskedMessage, throwable);
            return super.format(maskedEvent);
        }
        return super.format(event);
    }



}
