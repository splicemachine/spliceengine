/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.task;

import java.util.Map;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.splicemachine.SplicePlatform;

/**
 * Parameter specialization for export/import data utility.
 */
public class SpliceExportImportSqlToFileCommand extends WriteExportImportSqlToFileCommand {

    // Import/Export params.  We set to defaults initially and called setters override.

    /** Should export file be compresses? */
    private String exportCompress = SplicePlatform.EXPORT_COMPRESS_DEFAULT;
    /** File replication count */
    private String exportRepCnt = SplicePlatform.EXPORT_REP_CNT_DEFAULT;
    /** File character set encoding */
    private String exportImportFileEncoding = SplicePlatform.EXPORT_IMPORT_FILE_ENCODING_DEFAULT;
    /** File field separator. Must be same for import and export.  */
    private String exportImportFieldSep = SplicePlatform.EXPORT_IMPORT_FIELD_SEP_DEFAULT;
    /** File quote char. Must be same for import and export.  */
    private String exportImportQuoteChar = SplicePlatform.EXPORT_IMPORT_QUOTE_CHAR_DEFAULT;
    /** Import record fail threshold.  */
    private String importFailThreshold = SplicePlatform.IMPORT_FAIL_THRESHOLD_DEFAULT;
    /** Import records do not span multiple lines in data file.  Default (null) is true.  */
    private String importOneLineRecords = SplicePlatform.IMPORT_ONE_LINE_RECORDS_DEFAULT;

    /**
     * Specifies whether to compress the exported data file.
     *
     * @param exportCompress true or false
     * @ant.not-required
     */
    public void setExportCompress(String exportCompress) {
        this.exportCompress = exportCompress;
    }

    /**
     * Specifies the data field separator to use.  Escape as necessary.
     *
     * @param exportImportFieldSep field separator, a char.
     * @ant.not-required
     */
    public void setExportImportFieldSep(String exportImportFieldSep) {
        this.exportImportFieldSep = exportImportFieldSep;
    }


    /**
     * Specifies the data file encoding to use.
     *
     * @param exportImportFileEncoding java character encoding. Default is UTF-8.
     * @ant.not-required
     */
    public void setExportImportFileEncoding(String exportImportFileEncoding) {
        this.exportImportFileEncoding = exportImportFileEncoding;
    }


    /**
     * Specifies the data quote character to use.  Escape as necessary.
     *
     * @param exportImportQuoteChar quote char, a char.
     * @ant.not-required
     */
    public void setExportImportQuoteChar(String exportImportQuoteChar) {
        this.exportImportQuoteChar = exportImportQuoteChar;
    }


    /**
     * Specifies the replication count for exported file(s).
     *
     * @param exportRepCnt integer. Default is 1.
     * @ant.not-required
     */
    public void setExportRepCnt(String exportRepCnt) {
        this.exportRepCnt = exportRepCnt;
    }


    /**
     * Specifies the number of  allowable errors before import failures terminate import.
     *
     * @param importFailThreshold Default is 0. Unlimited is -1.
     * @ant.not-required
     */
    public void setImportFailThreshold(String importFailThreshold) {
        this.importFailThreshold = importFailThreshold;
    }

    /**
     * Specifies whether import data records can span multiple lines in the import data file.<br/>
     * Since this import is the result of a splice export, all records are writen to one line.
     * This field has no impact.
     *
     * @param oneLineRecords Default (null or unset) is true.
     * @ant.not-required
     */
    public void setImportOneLineRecords(String oneLineRecords) {
        this.importOneLineRecords = oneLineRecords;
    }

    protected Map<String, String> getExportImportParameters(Platform platform) {
        Map<String, String> params = super.getExportImportParameters(platform);
        params.put(SplicePlatform.EXPORT_COMPRESS, exportCompress);
        params.put(SplicePlatform.EXPORT_REP_CNT, exportRepCnt);
        params.put(SplicePlatform.EXPORT_IMPORT_FILE_ENCODING, (isDefault(exportImportFileEncoding) ? "null" : quote(exportImportFileEncoding)));
        params.put(SplicePlatform.EXPORT_IMPORT_FIELD_SEP, (isDefault(exportImportFieldSep) ? "null" : quote(exportImportFieldSep)));
        params.put(SplicePlatform.EXPORT_IMPORT_QUOTE_CHAR, (isDefault(exportImportQuoteChar) ? "null" : quote(exportImportQuoteChar)));
        params.put(SplicePlatform.IMPORT_FAIL_THRESHOLD, importFailThreshold);
        params.put(SplicePlatform.IMPORT_ONE_LINE_RECORDS, importOneLineRecords);

        return params;
    }

    private String quote(String param) {
        return "'"+param+"'";
    }

    private boolean isDefault(String param) {
        return (param == null || param.isEmpty());
    }
}
