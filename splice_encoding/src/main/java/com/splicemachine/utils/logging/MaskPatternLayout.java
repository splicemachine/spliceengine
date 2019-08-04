package com.splicemachine.utils.logging;

import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.List;
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
        Matcher matcher = maskPattern.matcher(message);

        List<Pair<Integer, Integer>> groups = new ArrayList<>();
        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                groups.add(new Pair<>(matcher.start(i), matcher.end(i)));
            }
        }
        if (groups.size() == 0) {
            return message;
        }
        StringBuilder maskedMessage = new StringBuilder("");
        for (int i = 0; i <= groups.size(); i++) {
            int start;
            int end;
            if (i == 0) {
                start = 0;
            } else {
                start = groups.get(i-1).getSecond();
                maskedMessage.append(maskString);
            }
            if (i == groups.size()) {
                end = message.length() - 1;
            } else {
                end = groups.get(i).getFirst() - 1;
            }
            maskedMessage.append(message.substring(start, end+1));
        }
        return String.valueOf(maskedMessage);
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
