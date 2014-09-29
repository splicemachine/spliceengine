package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents the user provided parameters of a given export.
 */
public class ExportParams {

    private static final String DEFAULT_ENCODING = Charsets.UTF_8.name();
    private static final short DEFAULT_REPLICATION_COUNT = 1;
    private static final ExportFileSystemType DEFAULT_FILE_SYSTEM = ExportFileSystemType.HDFS;
    private static final char DEFAULT_FIELD_DELIMITER = ',';
    private static final char DEFAULT_QUOTE_CHAR = '"';
    private static final String DEFAULT_RECORD_DELIMITER = "\n";

    private String directory;
    private ExportFileSystemType fileSystemType = DEFAULT_FILE_SYSTEM;
    private short replicationCount = DEFAULT_REPLICATION_COUNT;
    private String characterEncoding = DEFAULT_ENCODING;

    private char fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private char quoteChar = DEFAULT_QUOTE_CHAR;

    // for serialization
    public ExportParams() {
    }

    public ExportParams(String directory, String fileSystemType, int replicationCount, String characterEncoding,
                        String fieldDelimiter, String quoteChar) {
        setDirectory(directory);
        setFileSystemType(fileSystemType);
        setReplicationCount((short) replicationCount);
        setCharacterEncoding(characterEncoding);
        setDefaultFieldDelimiter(fieldDelimiter);
        setQuoteChar(quoteChar);
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

    public ExportFileSystemType getFileSystemType() {
        return fileSystemType;
    }

    public short getReplicationCount() {
        return replicationCount;
    }

    // - - - - - - - - - - -
    // private setters
    // - - - - - - - - - - -

    private void setDirectory(String directory) {
        checkArgument(!isNullOrEmpty(directory), " export directory is required");
        this.directory = directory;
    }

    private void setFileSystemType(String fileSystemType) {
        if (!isNullOrEmpty(fileSystemType)) {
            checkArgument(ExportFileSystemType.isValid(fileSystemType.toUpperCase()),
                    " invalid file system type '%s', valid values are: %s", fileSystemType,
                    Joiner.on(", ").join(ExportFileSystemType.values()));
            this.fileSystemType = ExportFileSystemType.valueOf(fileSystemType.toUpperCase());
        }
    }

    private void setReplicationCount(short replicationCount) {
        if (replicationCount > 0) {
            this.replicationCount = replicationCount;
        }
    }

    public void setCharacterEncoding(String characterEncoding) {
        if (!isNullOrEmpty(characterEncoding)) {
            this.characterEncoding = characterEncoding;
        }
    }

    public void setDefaultFieldDelimiter(String fieldDelimiter) {
        if (!isNullOrEmpty(fieldDelimiter)) {
            checkArgument(fieldDelimiter.length() == 1, " field delimiter must be a single character");
            this.fieldDelimiter = fieldDelimiter.charAt(0);
        }
    }

    public void setQuoteChar(String quoteChar) {
        if (!isNullOrEmpty(quoteChar)) {
            checkArgument(quoteChar.length() == 1, " quote character must be a single character");
            this.quoteChar = quoteChar.charAt(0);
        }
    }

    private boolean isNullOrEmpty(String directory) {
        return directory == null || directory.trim().length() == 0;
    }

}
