package com.splicemachine.db.iapi.reference;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This is a Validator for string options that need to be parsed.
 *
 * Examples for validators that implement Validator.Interface:
 * - GlobalDBProperties::parseBoolean (allow true/TRUE/false/FALSE)
 * - Integer::parseInt (allow integers)
 * - Double::parseDouble (allow doubles)
 * - Utils::getTimestampFormatLength (checks timestamp format)
 * - new MultipleOptionsValidator(new String[]{"on", "off", "forced"})) (allow on/off/forced).
 * - new RegexValidator("\\d*")
 * - new DoubleRange(0.0, 1.0)
 */
public class Validator {
    /**
     * Interface for all validators:
     *  If the option is not correct / not parsable, throw.
     *  otherwise, return normally
     */
    public interface Interface {
        void accept(String t) throws Throwable;
    }

    /**
     * new MultipleOptionsValidator(new String[]{"on", "off", "forced"})
     * default caseSensitive = false.
     */
    static class MultipleOptions implements Validator.Interface {
        String[] options;
        boolean caseSensitive = false;

        MultipleOptions(String[] options) {
            this.options = options;
        }

        MultipleOptions caseSensitive() {
            caseSensitive = true;
            return this;
        }

        @Override
        public void accept(String t) throws Throwable {
            Stream<String> s = Arrays.stream(options);
            if( caseSensitive )
                s = s.filter( t::equals );
            else
                s = s.filter( t::equalsIgnoreCase );
            if( s.count() != 0 )
                return;
            throw new RuntimeException("Supported values are " + Arrays.toString(options) + ".");
        }
    }

    /**
     * throws if input is not null, true, TRUE, false or FALSE.
     */
    public static void parseBoolean(String s) {
        if( s == null || (!s.equalsIgnoreCase("false") && !s.equalsIgnoreCase("true")) ) {
            throw new RuntimeException("Expected either TRUE or FALSE.");
        }
    }

    /**
     * e.g. new RegexValidator("\\d*"); -> Integer validator (however rather use Integer::parseInt)
     */
    static class RegexValidator implements Validator.Interface
    {
        Pattern pattern;
        RegexValidator(String regex) {
            pattern = Pattern.compile(regex);
        }
        @Override
        public void accept(String t) throws Throwable {
            if( !pattern.matcher(t).find())
                throw new RuntimeException("Did not match pattern " + pattern.pattern());
        }
    }

    /**
     * e.g. new DoubleRange(0.0, 1.0) -> only allow doubles in range [0.0, 1.0]
     * note that these are INCLUSIVE bounds.
     */
    static class DoubleRange implements Validator.Interface {
        double min, max;
        DoubleRange(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public void accept(String t) throws Throwable {
            double d;
            try {
                d = Double.parseDouble(t);
            } catch (Exception parseDoubleE) {
                throw new RuntimeException("could not parse Double value");
            }
            if (d < min || d > max) {
                throw new RuntimeException("Double value expected to be in range [" + min + ", " + max + "]");
            }

        }
    }
}
