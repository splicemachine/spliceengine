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

    private static final String DEFAULT_ENCODING = Charsets.UTF_8.name();
    private static final short DEFAULT_REPLICATION_COUNT = 1;
    private static final char DEFAULT_FIELD_DELIMITER = ',';
    private static final char DEFAULT_QUOTE_CHAR = '"';
    private static final String DEFAULT_RECORD_DELIMITER = "\n";

    private String directory;
    private String format;
    private short replicationCount = DEFAULT_REPLICATION_COUNT;
    private COMPRESSION compression;
    private String characterEncoding = DEFAULT_ENCODING;

    private char fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private char quoteChar = DEFAULT_QUOTE_CHAR;

    // for serialization
    public ExportParams() {
    }

    public ExportParams(String directory, String compression, String format, int replicationCount, String characterEncoding,
                        String fieldDelimiter, String quoteChar) throws StandardException {
        setDirectory(directory);
        setFormat(format);
        setCompression(compression);
        setReplicationCount((short) replicationCount);
        setCharacterEncoding(characterEncoding);
        setDefaultFieldDelimiter(StringEscapeUtils.unescapeJava(fieldDelimiter));
        setQuoteChar(StringEscapeUtils.unescapeJava(quoteChar));
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
            if (f.compareTo("PARQUET") == 0) {
                // Only support snappy compression for parquet
                if (compression.compareTo("SNAPPY") == 0 ||
                        compression.compareTo("TRUE") == 0) {
                    this.compression = COMPRESSION.SNAPPY;
                } else if (compression.compareTo("NONE") == 0 ||
                        compression.compareTo("FALSE") == 0) {
                    this.compression = COMPRESSION.NONE;
                } else throw StandardException.newException(SQLState.UNSUPPORTED_COMPRESSION_FORMAT, compression);
            } else if (f.compareTo("CSV") == 0) {
                // Support gzip, bzip2 for csv
                if (compression.compareTo("BZ2") == 0 ||
                        compression.compareTo("BZIP2") == 0) {
                    this.compression = COMPRESSION.BZ2;
                } else if (compression.compareTo("GZ") == 0 ||
                        compression.compareTo("GZIP") == 0 ||
                        compression.compareTo("TRUE") == 0) {
                    this.compression = COMPRESSION.GZ;
                } else if (compression.compareTo("NONE") == 0 ||
                        compression.compareTo("FALSE") == 0) {
                    this.compression = COMPRESSION.NONE;
                } else throw StandardException.newException(SQLState.UNSUPPORTED_COMPRESSION_FORMAT, compression);
            }
        }
        else
            this.compression = COMPRESSION.NONE;
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
