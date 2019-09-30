/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.si.constants;

import com.splicemachine.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Created by jleach on 12/9/15.
 */
@SuppressFBWarnings("MS_MUTABLE_ARRAY")
public class SIConstants {

    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = Bytes.toBytes("SPLICE_SEQUENCES");
    //snowflake stuff
    public static final String MACHINE_ID_COUNTER = "MACHINE_IDS";
    public static final byte[] COUNTER_COL = Bytes.toBytes("c");

    public static final int TRANSACTION_TABLE_BUCKET_COUNT = 16; //must be a power of 2
    public static final byte[] TRUE_BYTES = Bytes.toBytes(true);
    public static final byte[] FALSE_BYTES = Bytes.toBytes(false);
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] SNAPSHOT_ISOLATION_FAILED_TIMESTAMP = {-1};
    public static final int TRANSACTION_START_TIMESTAMP_COLUMN = 0;
    public static final int TRANSACTION_PARENT_COLUMN = 1;
    public static final int TRANSACTION_DEPENDENT_COLUMN = 2;
    public static final int TRANSACTION_ALLOW_WRITES_COLUMN = 3;
    public static final int TRANSACTION_READ_UNCOMMITTED_COLUMN = 4;
    public static final int TRANSACTION_READ_COMMITTED_COLUMN = 5;
    public static final int TRANSACTION_STATUS_COLUMN = 6;
    public static final int TRANSACTION_COMMIT_TIMESTAMP_COLUMN = 7;
    public static final int TRANSACTION_KEEP_ALIVE_COLUMN = 8;
    public static final int TRANSACTION_ID_COLUMN = 14;
    public static final int TRANSACTION_COUNTER_COLUMN = 15;
    public static final int TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN = 16;
    public static final long TRASANCTION_INCREMENT = 0x100l;
    public static final long SUBTRANSANCTION_ID_MASK = 0xFFl;
    public static final long TRANSANCTION_ID_MASK= 0xFFFFFFFFFFFFFF00l;

    /**
     * Splice Columns
     *
     * 0 = contains commit timestamp (optionally written after writing transaction is final)
     * 1 = tombstone (if value empty) or anti-tombstone (if value "0")
     * 7 = encoded user data
     * 9 = column for causing write conflicts between concurrent transactions writing to parent and child FK tables
     */
    public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes("0");
    public static final byte[] SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES = Bytes.toBytes("1");
    public static final byte[] SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES = Bytes.toBytes("9");
    public static final byte[] SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES = Bytes.toBytes("0");


    public static final String SI_TRANSACTION_KEY = "T";
    public static final String SI_TRANSACTION_ID_KEY = "A";
    public static final String SI_NEEDED = "B";
    public static final String SI_DELETE_PUT = "D";
    public static final String SI_COUNT_STAR = "M";

    //common SI fields
    public static final String NA_TRANSACTION_ID = "NA_TRANSACTION_ID";
    public static final String SI_EXEMPT = "si-exempt";

    public static final byte[] SI_NEEDED_VALUE_BYTES = Bytes.toBytes((short) 0);

    // The column in which splice stores encoded/packed user data.
    public static final byte[] PACKED_COLUMN_BYTES = Bytes.toBytes("7");

    public static final String DEFAULT_FAMILY_NAME = "V";

    public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes("V");

    public static final String SI_PERMISSION_FAMILY = "P";

    // Default Constants
    public static final String SUPPRESS_INDEXING_ATTRIBUTE_NAME = "iu";
    public static final byte[] SUPPRESS_INDEXING_ATTRIBUTE_VALUE = {};
    public static final String CHECK_BLOOM_ATTRIBUTE_NAME = "cb";

    public static final String TOKEN_ACL_NAME = "ta";

    public static final String ENTRY_PREDICATE_LABEL= "p";

    public static final int DEFAULT_CACHE_SIZE=1<<10;

    // Name of property to use for caching full display name of table and index.
    // Generic but ultimately used in hbase where we want these to be available
    // in HTableDescriptor.

    public static final String SCHEMA_DISPLAY_NAME_ATTR = "schemaDisplayName";
    public static final String TABLE_DISPLAY_NAME_ATTR = "tableDisplayName";
    public static final String INDEX_DISPLAY_NAME_ATTR = "indexDisplayName";
    public static final String TRANSACTION_ID_ATTR = "createTransactionId";
    public static final String DROPPED_TRANSACTION_ID_ATTR = "droppedTransactionId";

    public static final String OLAP_DEFAULT_QUEUE_NAME = "default";
    public static final String YARN_DEFAULT_QUEUE_NAME = "default";
}
