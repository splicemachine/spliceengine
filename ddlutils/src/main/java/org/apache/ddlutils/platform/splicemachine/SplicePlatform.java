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

package org.apache.ddlutils.platform.splicemachine;

import java.sql.Types;

import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.CascadeActionEnum;
import org.apache.ddlutils.platform.derby.DerbyPlatform;

/**
 * This is where splicemachine would implement splice-specific platform methods.
 */
public class SplicePlatform extends DerbyPlatform {
    /**
     * Database name of this platform.
     */
    public static final String DATABASENAME = "SpliceDB";
    /**
     * The splice jdbc driver for use as a client for a normal server.
     */
    public static final String JDBC_DRIVER = "com.splicemachine.db.jdbc.ClientDriver";
    /**
     * The splice jdbc driver for use as an embedded database.
     */
    public static final String JDBC_DRIVER_EMBEDDED = "com.splicemachine.db.jdbc.EmbeddedDriver";
    /**
     * The subprotocol used by the splice drivers.
     */
    public static final String JDBC_SUBPROTOCOL = "splice";

    // Import/Export param defaults
    /** Should export file be compresses? */
    public static final String EXPORT_COMPRESS_DEFAULT = "false";
    public static final String EXPORT_COMPRESS = "EXPORT_COMPRESS";
    /** File replication count */
    public static final String EXPORT_REP_CNT_DEFAULT = "1";
    public static final String EXPORT_REP_CNT = "EXPORT_REP_CNT";
    /** File character set encoding */
    public static final String EXPORT_IMPORT_FILE_ENCODING_DEFAULT = "UTF-8";
    public static final String EXPORT_IMPORT_FILE_ENCODING = "EXPORT_IMPORT_FILE_ENCODING";
    /** File field separator. Must be same for import and export.  */
    public static final String EXPORT_IMPORT_FIELD_SEP_DEFAULT = ",";
    public static final String EXPORT_IMPORT_FIELD_SEP = "EXPORT_IMPORT_FIELD_SEP";
    /** File quote char. Must be same for import and export.  */
    public static final String EXPORT_IMPORT_QUOTE_CHAR_DEFAULT = "\"";
    public static final String EXPORT_IMPORT_QUOTE_CHAR = "EXPORT_IMPORT_QUOTE_CHAR";
    /** Import record fail threshold.  */
    public static final String IMPORT_FAIL_THRESHOLD_DEFAULT = "0";
    public static final String IMPORT_FAIL_THRESHOLD = "IMPORT_FAIL_THRESHOLD";
    /** Import records do not span multiple lines. Default (null) is true. */
    public static final String IMPORT_ONE_LINE_RECORDS_DEFAULT = "true";
    public static final String IMPORT_ONE_LINE_RECORDS = "IMPORT_ONE_LINE_RECORDS";

    /**
     * Creates a new Splice platform instance.
     */
    public SplicePlatform() {
        super();

        PlatformInfo info = getPlatformInfo();

        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE");
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        info.setSupportedOnUpdateActions(new CascadeActionEnum[]{CascadeActionEnum.NONE, CascadeActionEnum.RESTRICT});
        info.setDefaultOnUpdateAction(CascadeActionEnum.NONE);
        info.addEquivalentOnUpdateActions(CascadeActionEnum.NONE, CascadeActionEnum.RESTRICT);
        info.setSupportedOnDeleteActions(new CascadeActionEnum[]{CascadeActionEnum.NONE, CascadeActionEnum.RESTRICT,
            CascadeActionEnum.CASCADE, CascadeActionEnum.SET_NULL});
        info.setDefaultOnDeleteAction(CascadeActionEnum.NONE);

        setSqlBuilder(new SpliceBuilder(this));
        setModelReader(new SpliceModelReader(this));
    }
}
