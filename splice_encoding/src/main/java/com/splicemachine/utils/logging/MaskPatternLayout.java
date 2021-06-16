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

import com.splicemachine.utils.StringUtils;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.layout.*;
import org.apache.logging.log4j.core.pattern.RegexReplacement;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.apache.logging.log4j.util.StringBuilders;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

@Plugin(name = "MaskPatternLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public final class MaskPatternLayout extends AbstractStringLayout {

    private PatternLayout patternLayout;
    private Encoder<StringBuilder> textEncoder;
    private String maskString = "********";
    private Pattern maskPattern = null;
    private Logger logger = LogManager.getLogger(this.getClass());
    protected static final int DEFAULT_STRING_BUILDER_SIZE = 1024;

    protected static final int MAX_STRING_BUILDER_SIZE = Math.max(DEFAULT_STRING_BUILDER_SIZE,
            size("log4j.layoutStringBuilder.maxSize", 2 * 1024));

    private MaskPatternLayout(PatternLayout patternLayout) {
        super(patternLayout.getConfiguration(), patternLayout.getCharset(),
              patternLayout.getHeaderSerializer(), patternLayout.getFooterSerializer());
        this.patternLayout = patternLayout;
    }

    private static int size(final String property, final int defaultValue) {
        return PropertiesUtil.getProperties().getIntegerProperty(property, defaultValue);
    }

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

    protected Encoder<StringBuilder> getStringBuilderEncoder() {
        if (textEncoder == null) {
            textEncoder = new StringBuilderEncoder(patternLayout.getCharset());
        }
        return textEncoder;
    }

    private StringBuilder toText(final AbstractStringLayout.Serializer2 serializer, final LogEvent event,
                                 final StringBuilder destination) {
        return serializer.toSerializable(event, destination);
    }

    protected static void trimToMaxSize(final StringBuilder stringBuilder) {
        StringBuilders.trimToMaxSize(stringBuilder, MAX_STRING_BUILDER_SIZE);
    }

    @Override
    public void encode(final LogEvent event, final ByteBufferDestination destination) {
        if (!(getEventSerializer() instanceof AbstractStringLayout.Serializer2)) {
            super.encode(event, destination);
            return;
        }
        final StringBuilder text = toText((AbstractStringLayout.Serializer2) getEventSerializer(), event, getStringBuilder());
        final Encoder<StringBuilder> encoder = getStringBuilderEncoder();
        String maskedMessage = maskMessage(text.toString(), maskPattern, maskString);
        final StringBuilder maskedStringBuilder = new StringBuilder();
        maskedStringBuilder.append(maskedMessage);
        encoder.encode(maskedStringBuilder, destination);
        trimToMaxSize(maskedStringBuilder);
    }


    @PluginBuilderFactory
    public static MaskPatternLayout.Builder newBuilder() {
        return new MaskPatternLayout.Builder();
    }

    public AbstractStringLayout.Serializer getEventSerializer() {
        return patternLayout.getEventSerializer();
    }

    @Override
    public String toSerializable(final LogEvent event) {
        return patternLayout.toSerializable(event);
    }

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<MaskPatternLayout> {
        private PatternLayout.Builder builder;

        private Builder() {
            builder = PatternLayout.newBuilder();
        }

        public MaskPatternLayout.Builder withPattern(final String pattern) {
            builder.withPattern(pattern);
            return this;
        }

        public MaskPatternLayout.Builder withPatternSelector(final PatternSelector patternSelector) {
            builder.withPatternSelector(patternSelector);
            return this;
        }

        public MaskPatternLayout.Builder withConfiguration(final Configuration configuration) {
            builder.withConfiguration(configuration);
            return this;
        }

        public MaskPatternLayout.Builder withRegexReplacement(final RegexReplacement regexReplacement) {
            builder.withRegexReplacement(regexReplacement);
            return this;
        }

        public MaskPatternLayout.Builder withCharset(final Charset charset) {
            builder.withCharset(charset);
            return this;
        }

        public MaskPatternLayout.Builder withAlwaysWriteExceptions(final boolean alwaysWriteExceptions) {
            builder.withAlwaysWriteExceptions(alwaysWriteExceptions);
            return this;
        }

        public MaskPatternLayout.Builder withDisableAnsi(final boolean disableAnsi) {
            builder.withDisableAnsi(disableAnsi);
            return this;
        }

        public MaskPatternLayout.Builder withNoConsoleNoAnsi(final boolean noConsoleNoAnsi) {
            builder.withNoConsoleNoAnsi(noConsoleNoAnsi);
            return this;
        }

        public MaskPatternLayout.Builder withHeader(final String header) {
            builder.withHeader(header);
            return this;
        }

        public MaskPatternLayout.Builder withFooter(final String footer) {
            builder.withFooter(footer);
            return this;
        }

        public MaskPatternLayout build() {
            PatternLayout patternLayout = builder.build();

            return new MaskPatternLayout(patternLayout);
        }
    }
}
