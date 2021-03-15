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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.types.FloatingPointDataType;
import splice.com.google.common.base.Charsets;
import org.apache.commons.lang3.StringEscapeUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportFile.COMPRESSION;
/**
 * Represents the user provided parameters of a given export.
 */
public class ExportParams implements Serializable {
    enum QuoteMode {
        ALWAYS, DEFAULT
    }

    private static final String DEFAULT_ENCODING = Charsets.UTF_8.name();
    private static final short DEFAULT_REPLICATION_COUNT = 1;
    private static final char DEFAULT_FIELD_DELIMITER = ',';
    private static final char DEFAULT_QUOTE_CHAR = '"';
    private static final String DEFAULT_RECORD_DELIMITER = "\n";
    private static QuoteMode DEFAULT_QUOTE_MODE = QuoteMode.DEFAULT;

    private String directory;
    private String format;
    private short replicationCount = DEFAULT_REPLICATION_COUNT;
    private COMPRESSION compression;
    private String characterEncoding = DEFAULT_ENCODING;

    private char fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private char quoteChar = DEFAULT_QUOTE_CHAR;
    private QuoteMode quoteMode = DEFAULT_QUOTE_MODE;
    private int floatingPointNotation = FloatingPointDataType.PLAIN;
    private String timestampFormat = CompilerContext.DEFAULT_TIMESTAMP_FORMAT;

    // for serialization
    public ExportParams() {
    }

    public ExportParams(String directory, String compression, String format, int replicationCount, String characterEncoding,
                        String fieldDelimiter, String quoteChar, String quoteMode, String floatingPointNotation, String timestampFormat) throws StandardException {
        setDirectory(directory);
        setFormat(format);
        setCompression(compression);
        setReplicationCount((short) replicationCount);
        setCharacterEncoding(characterEncoding);
        setDefaultFieldDelimiter(StringEscapeUtils.unescapeJava(fieldDelimiter));
        setQuoteChar(StringEscapeUtils.unescapeJava(quoteChar));
        setQuoteMode(quoteMode);
        setFloatingPointNotation(floatingPointNotation);
        setTimestampFormat(timestampFormat);
    }

    /**
     * Create params with all default options and the specified directory.
     */
    public static ExportParams withDirectory(String directory) {
        ExportParams params = new ExportParams();
        params.directory = directory;
        return params;
    }

    public String getDirectory() {
        return directory;
    }

    public String getFormat() {
        return format;
    }

    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public String getRecordDelimiter() {
        return DEFAULT_RECORD_DELIMITER;
    }

    public String getCharacterEncoding() {
        return characterEncoding;
    }

    public COMPRESSION getCompression() {
        return compression;
    }

    public short getReplicationCount() {
        return replicationCount;
    }

    public QuoteMode getQuoteMode() {
        return quoteMode;
    }

    public int getFloatingPointNotation() {
        return floatingPointNotation;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    // - - - - - - - - - - -
    // private setters
    // - - - - - - - - - - -

    private void setDirectory(String directory) throws StandardException {
        checkArgument(!isBlank(directory), "export path", directory);
        this.directory = directory;
    }


    private void setFormat(String format) throws StandardException {
        checkArgument(!isBlank(format), "format", format);
        this.format = format;
    }

    private void setCompression(String compression) throws StandardException {
        if (compression != null) {
            compression = compression.trim().toUpperCase();
        }
        if (compression!= null && compression.length() > 0) {
            String f = format.trim().toUpperCase();
            if (f.equals("PARQUET")) {
                // Only support snappy compression for parquet
                if (compression.equals("SNAPPY") ||
                        compression.equals("TRUE")) {
                    this.compression = COMPRESSION.SNAPPY;
                } else if (compression.equals("NONE") ||
                        compression.equals("FALSE")) {
                    this.compression = COMPRESSION.NONE;
                } else throw StandardException.newException(SQLState.UNSUPPORTED_COMPRESSION_FORMAT, compression);
            } else if (f.equals("CSV")) {
                // Support gzip, bzip2 for csv
                if (compression.equals("BZ2") ||
                        compression.equals("BZIP2")) {
                    this.compression = COMPRESSION.BZ2;
                } else if (compression.equals("GZ") ||
                        compression.equals("GZIP") ||
                        compression.equals("TRUE")) {
                    this.compression = COMPRESSION.GZ;
                } else if (compression.equals("NONE") ||
                        compression.equals("FALSE")) {
                    this.compression = COMPRESSION.NONE;
                } else throw StandardException.newException(SQLState.UNSUPPORTED_COMPRESSION_FORMAT, compression);
            }
        }
        else
            this.compression = COMPRESSION.NONE;
    }

    public void setQuoteMode(String quoteMode) throws StandardException {
        if (quoteMode != null) {
            quoteMode = quoteMode.trim().toUpperCase();
        }
        if (quoteMode != null && quoteMode.length() > 0) {
            if (quoteMode.equals("ALWAYS")) {
                this.quoteMode = QuoteMode.ALWAYS;
            } else if (quoteMode.equals("DEFAULT")) {
                this.quoteMode = QuoteMode.DEFAULT;
            } else {
                throw StandardException.newException(SQLState.UNSUPPORTED_QUOTE_MODE, quoteMode);
            }
        }
    }

    public void setFloatingPointNotation(String floatingPointNotation) throws StandardException {
        if (floatingPointNotation != null) {
            floatingPointNotation = floatingPointNotation.trim().toUpperCase();
        }
        if (floatingPointNotation != null && floatingPointNotation.length() > 0) {
            switch (floatingPointNotation) {
                case "PLAIN":
                    this.floatingPointNotation = FloatingPointDataType.PLAIN;
                    break;
                case "NORMALIZED":
                    this.floatingPointNotation = FloatingPointDataType.NORMALIZED;
                    break;
                default:
                    throw StandardException.newException(SQLState.UNSUPPORTED_FLOATING_POINT_NOTATION, floatingPointNotation);
            }
        }
    }

    public void setTimestampFormat(String timestampFormat) {
        if (!isBlank(timestampFormat)) {
            this.timestampFormat = timestampFormat;
        }
    }

    private void setReplicationCount(short replicationCount) {
        if (replicationCount > 0) {
            this.replicationCount = replicationCount;
        }
    }

    public void setCharacterEncoding(String characterEncoding) throws StandardException {
        if (!isBlank(characterEncoding)) {
            checkArgument(isValidCharacterSet(characterEncoding), "encoding", characterEncoding);
            this.characterEncoding = characterEncoding;
        }
    }

    public void setDefaultFieldDelimiter(String fieldDelimiter) throws StandardException {
        if (!isEmpty(fieldDelimiter)) {
            checkArgument(fieldDelimiter.length() == 1, "field delimiter", fieldDelimiter);
            this.fieldDelimiter = fieldDelimiter.charAt(0);
        }
    }

    public void setQuoteChar(String quoteChar) throws StandardException {
        if (!isEmpty(quoteChar)) {
            checkArgument(quoteChar.length() == 1, "quote character", quoteChar);
            this.quoteChar = quoteChar.charAt(0);
        }
    }

    private static void checkArgument(boolean isOk, String parameter, String value) throws StandardException {
        if (!isOk) {
            throw StandardException.newException(SQLState.UU_INVALID_PARAMETER, parameter, value);
        }
    }

    public static boolean isValidCharacterSet(String charSet) {
        try {
            Charset.forName(charSet);
        } catch (UnsupportedCharsetException e) {
            return false;
        }
        return true;
    }

}
