/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.backup;

/**
 * Created by jyuan on 8/8/16.
 */
import org.apache.hadoop.hbase.util.Bytes;

public class BackupRestoreConstants {

    public static String BACKUP_DIR = "backup";
    public static final String BACKUP_RECORD_FILE_NAME = "SYSBACKUP";
    public static final String RESTORE_RECORD_FILE_NAME = "SYSRESTORE";
    public static final String REGION_FILE_NAME = ".regioninfo";
    public static final String ARCHIVE_DIR = "archive";
    public static final byte[] BACKUP_TYPE_FULL_BYTES = Bytes.toBytes("FULL");
    public static final byte[] BACKUP_TYPE_INCR_BYTES = Bytes.toBytes("INCR");
}
