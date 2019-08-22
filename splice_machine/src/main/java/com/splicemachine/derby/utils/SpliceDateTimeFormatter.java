/*
 * Copyright (c) 2018-2019 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.reference.SQLState;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.time.temporal.ChronoField.*;

public class SpliceDateTimeFormatter {

    public enum FormatterType {
        DATE,
        TIMESTAMP,
        TIME
    }

    public static final String defaultDateFormatString = "yyyy-MM-dd";
    public static final String defaultTimeFormatString = "HH:mm:ss";
    public static final String defaultTimestampFormatString = "yyyy-MM-dd HH:mm:ss";

    public static SpliceDateTimeFormatter DEFAULT_DATE_FORMATTER =
                  SpliceDateTimeFormatter.of(FormatterType.DATE);
    public static SpliceDateTimeFormatter DEFAULT_TIME_FORMATTER =
                  SpliceDateTimeFormatter.of(FormatterType.TIME);
    public static SpliceDateTimeFormatter DEFAULT_TIMESTAMP_FORMATTER =
                  SpliceDateTimeFormatter.of(FormatterType.TIMESTAMP);

    private FormatterType formatterType;
    private final String format;
    private java.time.format.DateTimeFormatter formatter = null;
    private boolean hasTimezoneOffset = false;
    private boolean needsTsRemoved = false;

    public boolean needsTsRemoved() { return needsTsRemoved; }
    public boolean hasTimezoneOffset() { return hasTimezoneOffset; }
    public boolean isDateOnlyFormat() { return formatterType == FormatterType.DATE; }
    public boolean isTimeOnlyFormat() { return formatterType == FormatterType.TIME; }
    public void setTimeOnlyFormat(boolean timeOnly) throws SQLException {
        if (timeOnly) {
            if (formatterType == FormatterType.DATE)
                throw new SQLException("Error parsing datetime with pattern: "+this.format+". Try using an" +
                    " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd",
                    SQLState.LANG_DATE_SYNTAX_EXCEPTION);

            formatterType = FormatterType.TIME;
            this.formatter = new DateTimeFormatterBuilder().append(formatter).
                                 parseDefaulting(MONTH_OF_YEAR, LocalDateTime.now().getMonth().getValue()).
                                 parseDefaulting(DAY_OF_MONTH, LocalDateTime.now().getDayOfMonth()).
                                 parseDefaulting(YEAR, LocalDateTime.now().getYear()).toFormatter();
        }
        formatterType = FormatterType.TIMESTAMP;
    }

    public void setDateOnlyFormat(boolean dateOnly) throws SQLException {
        if (dateOnly) {
            if (formatterType == FormatterType.TIME)
                throw new SQLException("Error parsing datetime with pattern: "+this.format+". Try using an" +
                    " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd",
                    SQLState.LANG_DATE_SYNTAX_EXCEPTION);

            formatterType = FormatterType.DATE;
        }
        formatterType = FormatterType.TIMESTAMP;
    }

    public static SpliceDateTimeFormatter of(FormatterType formatterType) {
        return new SpliceDateTimeFormatter(null, formatterType);
    }

/**
 * Splice formatter for parsing date, time and timestamp strings for
 * the TO_DATE, TO_TIME and TO_TIMESTAMP functions
 * and in IMPORT statements.
 * <p>
 * This class provides the main application entry point for parsing
 * of dates, times and timestamps with a specified format such as
 * {@code yyyy-MM-dd}.
 * <p>
 * See https://doc.splicemachine.com/sqlref_builtinfcns_todate.html for
 * information on supported Splice Machine date/timestamp format strings.
 */
    public SpliceDateTimeFormatter(String format, FormatterType formatterType) {

        this.formatterType = formatterType;

        // The default external format of "yyyy-MM-dd" may be used
        // in error messages, but the corresponding internally used format
        // is "yyyy-M-d".
        if (format == null) {
            this.formatterType = formatterType;
            if (formatterType == FormatterType.DATE) {
                format = "yyyy-M-d";
                this.format = defaultDateFormatString;
            }
            else if (formatterType == FormatterType.TIMESTAMP) {
                format = "yyyy-M-d [H][:m][:s].SSSSSS";
                this.format = defaultTimestampFormatString;
                needsTsRemoved = true;
            }
            else if (formatterType == FormatterType.TIME) {
                format = "H:m:s";
                this.format = defaultTimeFormatString;
            }
            else {
                // Leave the formatter uninitialized.
                // Using this undefined format should error out.
                this.format = null;
                return;
            }
        }
        else {
            this.format = format;

            // All non-default format strings may contain elements of both date and time,
            // so need to start out categorized as FormatterType.TIMESTAMP.
            this.formatterType = FormatterType.TIMESTAMP;

            // The valid format characters of DateTimeFormatter documented at the following link
            // can be used to identify separator characters like '-' or '/' :
            // See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

            // If no AM/PM marker, allow hh to map to HH.
            String AMPMFormatString = "(?<!['].{0,30})([a]+)";
            Pattern AMPMPattern = Pattern.compile("(?<!['])([a]+)");
            Matcher AMPMMatcher = AMPMPattern.matcher(format);
            if (!AMPMMatcher.find())
                format = format.replaceAll("h", "H");

            // Only a single 'a' supported for matching AM/PM.
            format = format.replaceFirst(AMPMFormatString, "a");

            // 'e' and 'c' capture the day of week 1 day off.
            // Need to use 'eee' or 'ccc' instead.
            String dayOfWeekFormatString = "(?<!['].{0,30})([c]+)";
            format = format.replaceAll(dayOfWeekFormatString, "ccc");
            dayOfWeekFormatString = "(?<!['].{0,30})([e]+)";
            format = format.replaceAll(dayOfWeekFormatString, "eee");

            // Replace .ssssss with .SSSSSS at the end of the format string.
            // This change is problematic for things like HH.mm.ss.
            // Probably won't support .ssssss, but keep this here
            // for now in case we want to.
//            String fracSecsRegex = "(\\.[s]+)(?!.*\\.[s]+.*)";
//            Pattern fractionalSecsPattern = Pattern.compile(fracSecsRegex);
//            Matcher fractionalSecsMatcher = fractionalSecsPattern.matcher(format);
//
//            if (fractionalSecsMatcher.find()) {
//                String fracSeconds = fractionalSecsMatcher.group(1);
//                fracSeconds = fracSeconds.replace('s','S');
//                format = format.replaceFirst(fracSecsRegex, fracSeconds);
//            }

            // Replace MM with M in the date format string.
            // Replace dd with d in the date format string.
            // Replace HH with H in the date format string.
            // Replace hh with h in the date format string.
            // Replace mm with m in the date format string.
            // Replace ss with s in the date format string.
            format = replaceDoubleCharWithSingle(format, 'M');
            format = replaceDoubleCharWithSingle(format, 'd');
            format = replaceDoubleCharWithSingle(format, 'H');
            format = replaceDoubleCharWithSingle(format, 'h');
            format = replaceDoubleCharWithSingle(format, 'm');
            format = replaceDoubleCharWithSingle(format, 's');

            // Preserve the old Joda zone offset 'Z' behavior by using
            // optional X patterns.
            format = format.replaceFirst("(?<!['Z\\[].{0,30})([Z]+)", "[XXX][X]");

            // Any X pattern should be replaced with the most flexible X pattern
            // of [XXX][X], unless the provided pattern is already optional (within brackets).
            format = format.replaceFirst("(?<!['X\\[].{0,30})([X]+)", "[XXX][X]");

        }
        // Z, z, V, O, X or x indicates time zone offset, unless it's preceded by a single quote.
        Pattern timeOffsetPattern = Pattern.compile("(?<!['Z])[Z]+");
        Matcher timeOffsetMatcher1 = timeOffsetPattern.matcher(format);
        timeOffsetPattern = Pattern.compile("(?<!['X])[X]+");
        Matcher timeOffsetMatcher2 = timeOffsetPattern.matcher(format);
        timeOffsetPattern = Pattern.compile("(?<!['x])[x]+");
        Matcher timeOffsetMatcher3 = timeOffsetPattern.matcher(format);
        timeOffsetPattern = Pattern.compile("(?<!['V])[V]+");
        Matcher timeOffsetMatcher4 = timeOffsetPattern.matcher(format);
        timeOffsetPattern = Pattern.compile("(?<!['O])[O]+");
        Matcher timeOffsetMatcher5 = timeOffsetPattern.matcher(format);
        timeOffsetPattern = Pattern.compile("(?<!['z])[z]+");
        Matcher timeOffsetMatcher6 = timeOffsetPattern.matcher(format);

        if (timeOffsetMatcher1.find() ||
            timeOffsetMatcher2.find() ||
            timeOffsetMatcher3.find() ||
            timeOffsetMatcher4.find() ||
            timeOffsetMatcher5.find() ||
            timeOffsetMatcher6.find())
            hasTimezoneOffset = true;
        try {
            // Need to make trailing S's in the format string
            // be optional.
            Pattern fractionalSecondsPattern = Pattern.compile("(\\.[S]+)");
            Matcher fractionalSecondsMatcher = fractionalSecondsPattern.matcher(format);
            if (fractionalSecondsMatcher.find())
            {
                String[] patternStrings = fractionalSecondsPattern.split(format);
                String fracSeconds = fractionalSecondsMatcher.group(1);
                int numDigits = fracSeconds.length() - 1;

                // Can't handle more than 6 fractional digits.
                // Set formatter to null so it will throw LANG_DATE_SYNTAX_EXCEPTION.
                if (numDigits > 6)
                    this.formatter = null;
                else {
                    DateTimeFormatterBuilder formatterBuilder =
                        new DateTimeFormatterBuilder();
                    boolean fracSecondsAppended = false;

                    for (String pattern:patternStrings) {
                        if (!fracSecondsAppended &&
                             format.indexOf(pattern) > fractionalSecondsMatcher.start()) {
                            fracSecondsAppended = true;
                            formatterBuilder = formatterBuilder.
                                appendFraction(ChronoField.MICRO_OF_SECOND,
                                0, numDigits, true);
                        }
                        formatterBuilder = formatterBuilder.appendPattern(pattern);
                    }
                    if (!fracSecondsAppended)
                        formatterBuilder = formatterBuilder.
                            appendFraction(ChronoField.MICRO_OF_SECOND,
                            0, numDigits, true);
                    this.formatter = formatterBuilder.toFormatter();
                }
            }
            else
                this.formatter = DateTimeFormatter.ofPattern(format);

            this.formatter = new DateTimeFormatterBuilder().parseLenient().
                                     append(this.formatter).
                                      toFormatter();
        }
        catch (IllegalArgumentException e) {
            // If the format is bad, this.formatter will be null, and the next time
            // getFormatter() is called we'll throw a SQLException.
        }
    }

    /**
     * Returns the original date format used in the SQL (such as in a TO_DATE function).
     * This may differ from the internal format used inside this class wh
     *
     * @param  none
     * @return The date format string, such as {@code yyyy-MM-dd}
     * @notes The returned format may differ from the internal format used inside
     *        this class.  This class utilizes the java.time.format.DateTimeFormatter
     *        class internally, but with a different internal format necessary
     *        to match the functionality documented for Splice date formats:
     *        https://doc.splicemachine.com/sqlref_builtinfcns_todate.html
     */
    public String getFormat() {
        return format;
    }

    public DateTimeFormatter getFormatter() throws SQLException {
        if (formatter == null)
            throw new SQLException("Error parsing datetime with pattern: "+this.format+". Try using an" +
                " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd",
                SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        return formatter;
    }

    public void setFormatter(DateTimeFormatter newFormatter) {formatter = newFormatter;}

    private static String replaceDoubleCharWithSingle(String format, char formarChar) {

        // Any non-alphabetic character may be a separator character.
        // This pattern matches one or more non-format characters.
        String
        excludeFormatCharsPattern = "([^A-Za-z]+)";

        // The string to replace ("MM" or "dd") may occur at the beginning or
        // end of the format string, with no characters preceding it
        // or following it.

        // We must not replace "MM" or "dd" if its neighbor is a
        // format character.
        // This pattern excludeFormatCharsPattern or the empty string
        // must be present on both sides of the match string.

        String matchPattern;
        matchPattern = format("(^[%c])([%c])%s", formarChar, formarChar, excludeFormatCharsPattern);
        format = format.replaceFirst(matchPattern, "$1$3");
        matchPattern = format("%s([%c])([%c]$)", excludeFormatCharsPattern, formarChar, formarChar);
        format = format.replaceFirst(matchPattern, "$1$3");
        matchPattern = format("%s([%c])([%c])%s", excludeFormatCharsPattern, formarChar, formarChar,
                                                  excludeFormatCharsPattern);
        format = format.replaceFirst(matchPattern, "$1$3$4");

        return format;
    }
}