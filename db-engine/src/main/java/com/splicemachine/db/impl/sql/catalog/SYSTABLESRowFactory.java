/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import splice.com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Factory for creating a SYSTABLES row.
 *
 * @version 0.1
 */
public class SYSTABLESRowFactory extends CatalogRowFactory {
    public static final String TABLENAME_STRING = "SYSTABLES";

    public static final int SYSTABLES_COLUMN_COUNT = 16;

    /* Column #s for systables (1 based) */
    public    static final int SYSTABLES_TABLEID                = 1;
    public    static final int SYSTABLES_TABLENAME              = 2;
    protected static final int SYSTABLES_TABLETYPE              = 3;
    public    static final int SYSTABLES_SCHEMAID               = 4;
    protected static final int SYSTABLES_LOCKGRANULARITY        = 5;
    protected static final int SYSTABLES_VERSION                = 6;
    /* Sequence for understanding coding/decoding with altering tables*/
    protected static final int SYSTABLES_COLUMN_SEQUENCE        = 7;
    /* External Tables Columns */
    protected static final int SYSTABLES_DELIMITED_BY           = 8;
    protected static final int SYSTABLES_ESCAPED_BY             = 9;
    protected static final int SYSTABLES_LINES_BY               = 10;
    protected static final int SYSTABLES_STORED_AS              = 11;
    protected static final int SYSTABLES_LOCATION               = 12;
    protected static final int SYSTABLES_COMPRESSION            = 13;
    // SYSTABLES_IS_PINNED : NOT USED ANYMORE, for backward compatibility only
    @Deprecated
    protected static final int SYSTABLES_IS_PINNED              = 14;
    protected static final int SYSTABLES_PURGE_DELETED_ROWS     = 15;
    protected static final int SYSTABLES_MIN_RETENTION_PERIOD   = 16;
    /* End External Tables Columns */

    protected static final int SYSTABLES_INDEX1_ID        = 0;
    protected static final int SYSTABLES_INDEX1_TABLENAME = 1;
    protected static final int SYSTABLES_INDEX1_SCHEMAID  = 2;

    protected static final int SYSTABLES_INDEX2_ID        = 1;
    protected static final int SYSTABLES_INDEX2_TABLEID   = 1;

    /* Column names */
    public static final String IS_PINNED            = "IS_PINNED";
    public static final String COMPRESSION          = "COMPRESSION";
    public static final String LOCATION             = "LOCATION";
    public static final String STORED               = "STORED";
    public static final String LINES                = "LINES";
    public static final String ESCAPED              = "ESCAPED";
    public static final String DELIMITED            = "DELIMITED";
    public static final String COLSEQUENCE          = "COLSEQUENCE";
    public static final String VERSION              = "VERSION";
    public static final String LOCKGRANULARITY      = "LOCKGRANULARITY";
    public static final String SCHEMAID             = "SCHEMAID";
    public static final String TABLETYPE            = "TABLETYPE";
    public static final String TABLENAME            = "TABLENAME";
    public static final String TABLEID              = "TABLEID";
    public static final String PURGE_DELETED_ROWS   = "PURGE_DELETED_ROWS";
    public static final String MIN_RETENTION_PERIOD = "MIN_RETENTION_PERIOD";
    /*
     * The first version of any tables. Use this for System tables and
     * any time that you don't know what the version is.
     */
    public static final String ORIGINAL_TABLE_VERSION = "1.0";
    //the current version for creating new tables with
    public static final String CURRENT_TABLE_VERSION = "4.0";

    // all indexes are unique.

    private static final String[] uuids =
        {
            "80000018-00d0-fd77-3ed8-000a0a0b1900", // catalog UUID
            "80000028-00d0-fd77-3ed8-000a0a0b1900", // heap UUID
            "8000001a-00d0-fd77-3ed8-000a0a0b1900", // SYSTABLES_INDEX1
            "8000001c-00d0-fd77-3ed8-000a0a0b1900", // SYSTABLES_INDEX2
        };

    private static final int[][] indexColumnPositions =
        {
            {SYSTABLES_TABLENAME, SYSTABLES_SCHEMAID},
            {SYSTABLES_TABLEID}
        };


    /////////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    SYSTABLESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSTABLES_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, (boolean[]) null, uuids);
    }

    SYSTABLESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
    {
        super(uuidf,ef,dvf, dd);
        initInfo(SYSTABLES_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, (boolean[]) null, uuids);
    }

    /////////////////////////////////////////////////////////////////////////////
    //
    //	METHODS
    //
    /////////////////////////////////////////////////////////////////////////////

    /**
     * Make a SYSTABLES row
     *
     * @throws StandardException thrown on failure
     * @return Row suitable for inserting into SYSTABLES.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "DB-9844")
    public ExecRow makeRow(boolean latestVersion,
                           TupleDescriptor td,
                           TupleDescriptor parent)
            throws StandardException {
        UUID oid;
        String tabSType = null;
        int tabIType;
        ExecRow row;
        String lockGranularity = null;
        String tableID = null;
        String schemaID = null;
        String tableName = null;
        int columnSequence = 0;
        String delimited = null;
        String escaped = null;
        String lines = null;
        String storedAs = null;
        String location = null;
        String compression = null;
        SQLVarchar tableVersion = null;
        // NOT USED ANYMORE, for backward compatibility only
        @Deprecated
        boolean isPinned = false;
        boolean purgeDeletedRows = false;
        Long minRetentionPeriod = null;

        if (td != null) {
            /*
             ** We only allocate a new UUID if the descriptor doesn't already have one.
             ** For descriptors replicated from a Source system, we already have an UUID.
             */
            if (!(td instanceof TableDescriptor))
                throw new RuntimeException("Unexpected TableDescriptor " + td.getClass().getName());
            if (!(parent instanceof SchemaDescriptor))
                throw new RuntimeException("Unexpected SchemaDescriptor " + parent.getClass().getName());
            TableDescriptor descriptor = (TableDescriptor) td;
            SchemaDescriptor schema = (SchemaDescriptor) parent;

            columnSequence = descriptor.getColumnSequence();
            oid = descriptor.getUUID();
            if (oid == null) {
                oid = getUUIDFactory().createUUID();
                descriptor.setUUID(oid);
            }
            tableID = oid.toString();

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(schema != null,
                        "Schema should not be null unless empty row is true");
                if (schema.getUUID() == null) {
                    SanityManager.THROWASSERT("schema " + schema + " has a null OID");
                }
            }

            schemaID = schema.getUUID().toString();

            tableName = descriptor.getName();

            /* RESOLVE - Table Type should really be a char in the descriptor
             * T, S, V, S instead of 0, 1, 2, 3
             */
            tabIType = descriptor.getTableType();
            switch (tabIType) {
                case TableDescriptor.BASE_TABLE_TYPE:
                    tabSType = "T";
                    break;
                case TableDescriptor.SYSTEM_TABLE_TYPE:
                    tabSType = "S";
                    break;
                case TableDescriptor.VIEW_TYPE:
                    tabSType = "V";
                    break;

                case TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE:
                    tabSType = "X";
                    break;

                case TableDescriptor.SYNONYM_TYPE:
                    tabSType = "A";
                    break;
                case TableDescriptor.EXTERNAL_TYPE:
                    tabSType = "E";
                    break;


                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("invalid table type");
            }
            char[] lockGChar = new char[1];
            lockGChar[0] = descriptor.getLockGranularity();
            lockGranularity = new String(lockGChar);
            delimited = descriptor.getDelimited();
            escaped = descriptor.getEscaped();
            lines = descriptor.getLines();
            storedAs = descriptor.getStoredAs();
            location = descriptor.getLocation();
            compression = descriptor.getCompression();
            //NOT USED ANYMORE, for backward compatibility only
            isPinned = descriptor.isPinned();
            purgeDeletedRows = descriptor.purgeDeletedRows();
            minRetentionPeriod = descriptor.getMinRetainedVersions();
            tableVersion = descriptor.getVersion() == null ?
                    new SQLVarchar(CURRENT_TABLE_VERSION) :
                    new SQLVarchar(descriptor.getVersion());
        } else
            tableVersion = new SQLVarchar(CURRENT_TABLE_VERSION);

        /* Insert info into systables */

        /* RESOLVE - It would be nice to require less knowledge about systables
         * and have this be more table driven.
         */

        /* Build the row to insert  */
        row = getExecutionFactory().getValueRow(SYSTABLES_COLUMN_COUNT);

        setRowColumns(row, tableID, tableName, tabSType, schemaID, lockGranularity, tableVersion, columnSequence,
                delimited, escaped, lines, storedAs, location, compression, isPinned, purgeDeletedRows, minRetentionPeriod);
        return row;
    }

    public static void setRowColumns(ExecRow row,
                                     String tableID,
                                     String tableName,
                                     String tabSType,
                                     String schemaID,
                                     String lockGranularity,
                                     SQLVarchar tableVersion,
                                     int columnSequence,
                                     String delimited,
                                     String escaped,
                                     String lines,
                                     String storedAs,
                                     String location,
                                     String compression,
                                     boolean isPinned,
                                     boolean purgeDeletedRows,
                                     Long minRetentionPeriod) {
        /* 1st column is TABLEID (UUID - char(36)) */
        row.setColumn(SYSTABLES_TABLEID, new SQLChar(tableID));

        /* 2nd column is NAME (varchar(30)) */
        row.setColumn(SYSTABLES_TABLENAME, new SQLVarchar(tableName));

        /* 3rd column is TABLETYPE (char(1)) */
        row.setColumn(SYSTABLES_TABLETYPE, new SQLChar(tabSType));

        /* 4th column is SCHEMAID (UUID - char(36)) */
        row.setColumn(SYSTABLES_SCHEMAID, new SQLChar(schemaID));

        /* 5th column is LOCKGRANULARITY (char(1)) */
        row.setColumn(SYSTABLES_LOCKGRANULARITY, new SQLChar(lockGranularity));

        /* 6th column is VERSION (varchar(128)) */
        row.setColumn(SYSTABLES_VERSION, tableVersion);

        row.setColumn(SYSTABLES_COLUMN_SEQUENCE, new SQLInteger(columnSequence));

        row.setColumn(SYSTABLES_DELIMITED_BY, new SQLVarchar(delimited));
        row.setColumn(SYSTABLES_ESCAPED_BY, new SQLVarchar(escaped));
        row.setColumn(SYSTABLES_LINES_BY, new SQLVarchar(lines));
        row.setColumn(SYSTABLES_STORED_AS, new SQLVarchar(storedAs));
        row.setColumn(SYSTABLES_LOCATION, new SQLVarchar(location));
        row.setColumn(SYSTABLES_COMPRESSION, new SQLVarchar(compression));
        //NOT USED ANYMORE, for backward compatibility only
        row.setColumn(SYSTABLES_IS_PINNED, new SQLBoolean(isPinned));
        row.setColumn(SYSTABLES_PURGE_DELETED_ROWS, new SQLBoolean(purgeDeletedRows));
        row.setColumn(SYSTABLES_MIN_RETENTION_PERIOD, new SQLLongint(minRetentionPeriod));
    }

    /**
     * Builds an empty index row.
     *
     * @param rowLocation Row location for last column of index row
     * @return corresponding empty index row
     * @throws StandardException thrown on failure
     * @param    indexNumber    Index to build empty row for.
     */
    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT", justification = "DB-9844")
    ExecIndexRow buildEmptyIndexRow(int indexNumber,
                                    RowLocation rowLocation)
            throws StandardException {
        int ncols = getIndexColumnCount(indexNumber);
        ExecIndexRow row = getExecutionFactory().getIndexableRow(ncols + 1);

        row.setColumn(ncols + 1, rowLocation);

        switch (indexNumber) {
            case SYSTABLES_INDEX1_ID:
                /* 1st column is TABLENAME (varchar(128)) */
                row.setColumn(1, new SQLVarchar());

                /* 2nd column is SCHEMAID (UUID - char(36)) */
                row.setColumn(2, new SQLChar());

                break;

            case SYSTABLES_INDEX2_ID:
                /* 1st column is TABLEID (UUID - char(36)) */
                row.setColumn(1, new SQLChar());
                break;

            default:
                throw new IllegalArgumentException("unexpected indexNumber: " + indexNumber);
        }    // end switch

        return row;
    }

    /**
     * Make a TableDescriptor out of a SYSTABLES row
     *
     * @param row                   a SYSTABLES row
     * @param parentTupleDescriptor Null for this kind of descriptor.
     * @param dd                    dataDictionary
     * @param isolationLevel        use this explicit isolation level. Only
     *                              ISOLATION_REPEATABLE_READ (normal usage)
     *                              or ISOLATION_READ_UNCOMMITTED (corner
     *                              cases) supported for now.
     * @throws StandardException thrown on failure
     */
    TupleDescriptor buildDescriptor(
            ExecRow row,
            TupleDescriptor parentTupleDescriptor,
            DataDictionary dd,
            int isolationLevel)
            throws StandardException {
        return buildDescriptorBody(row,
                parentTupleDescriptor,
                dd,
                isolationLevel);
    }


    ///////////////////////////////////////////////////////////////////////////
    //
    //	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make a TableDescriptor out of a SYSTABLES row
     *
     * @param row                   a SYSTABLES row
     * @param parentTupleDescriptor Null for this kind of descriptor.
     * @param dd                    dataDictionary
     * @throws StandardException thrown on failure
     * @return a table descriptor equivalent to a SYSTABLES row
     */

    public TupleDescriptor buildDescriptor(
            ExecRow row,
            TupleDescriptor parentTupleDescriptor,
            DataDictionary dd)
            throws StandardException {
        return buildDescriptorBody(
                row,
                parentTupleDescriptor,
                dd,
                TransactionController.ISOLATION_REPEATABLE_READ);
    }


    public TupleDescriptor buildDescriptorBody(
            ExecRow row,
            TupleDescriptor parentTupleDescriptor,
            DataDictionary dd,
            int isolationLevel)
            throws StandardException {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(row.nColumns() == SYSTABLES_COLUMN_COUNT, "Wrong number of columns for a SYSTABLES row");

        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        String tableUUIDString;
        String schemaUUIDString;
        int tableTypeEnum;
        String lockGranularity;
        String tableName, tableType;
        DataValueDescriptor col;
        UUID tableUUID;
        UUID schemaUUID;
        SchemaDescriptor schema;
        TableDescriptor tabDesc;

        /* 1st column is TABLEID (UUID - char(36)) */
        col = row.getColumn(SYSTABLES_TABLEID);
        tableUUIDString = col.getString();
        tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);


        /* 2nd column is TABLENAME (varchar(128)) */
        col = row.getColumn(SYSTABLES_TABLENAME);
        tableName = col.getString();

        /* 3rd column is TABLETYPE (char(1)) */
        col = row.getColumn(SYSTABLES_TABLETYPE);
        tableType = col.getString();
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(tableType.length() == 1, "Fourth column type incorrect");
        }
        switch (tableType.charAt(0)) {
            case 'T':
                tableTypeEnum = TableDescriptor.BASE_TABLE_TYPE;
                break;
            case 'S':
                tableTypeEnum = TableDescriptor.SYSTEM_TABLE_TYPE;
                break;
            case 'V':
                tableTypeEnum = TableDescriptor.VIEW_TYPE;
                break;
            case 'A':
                tableTypeEnum = TableDescriptor.SYNONYM_TYPE;
                break;
            case 'X':
                tableTypeEnum = TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE;
                break;
            case 'E':
                tableTypeEnum = TableDescriptor.EXTERNAL_TYPE;
                break;
            default:
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Fourth column value invalid");
                tableTypeEnum = -1;
        }

        /* 4th column is SCHEMAID (UUID - char(36)) */
        col = row.getColumn(SYSTABLES_SCHEMAID);
        schemaUUIDString = col.getString();
        schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);

        schema = dd.getSchemaDescriptor(schemaUUID, isolationLevel, null);

        // If table is temp table, (SESSION) schema will be null
        if (schema == null && (tableTypeEnum == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)) {
            schema = dd.getDeclaredGlobalTemporaryTablesSchemaDescriptor();
        }

        /* 5th column is LOCKGRANULARITY (char(1)) */
        col = row.getColumn(SYSTABLES_LOCKGRANULARITY);
        lockGranularity = col.getString();
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(lockGranularity.length() == 1, "Fifth column type incorrect");
        }

        //TODO -sf- place version into tuple descriptor
        DataValueDescriptor versionDescriptor = row.getColumn(SYSTABLES_VERSION);

        DataValueDescriptor delimitedDVD = row.getColumn(SYSTABLES_DELIMITED_BY);
        DataValueDescriptor escapedDVD = row.getColumn(SYSTABLES_ESCAPED_BY);
        DataValueDescriptor linesDVD = row.getColumn(SYSTABLES_LINES_BY);
        DataValueDescriptor storedDVD = row.getColumn(SYSTABLES_STORED_AS);
        DataValueDescriptor locationDVD = row.getColumn(SYSTABLES_LOCATION);
        DataValueDescriptor compressionDVD = row.getColumn(SYSTABLES_COMPRESSION);
        // NOT USED ANYMORE, for backward compatibility only
        @Deprecated
        DataValueDescriptor isPinnedDVD = row.getColumn(SYSTABLES_IS_PINNED);
        DataValueDescriptor purgeDeletedRowsDVD = row.getColumn(SYSTABLES_PURGE_DELETED_ROWS);
        DataValueDescriptor minRetentionPeriodDVD = row.getColumn(SYSTABLES_MIN_RETENTION_PERIOD);

        // RESOLVE - Deal with lock granularity
        tabDesc = ddg.newTableDescriptor(tableName, schema, tableTypeEnum, lockGranularity.charAt(0),
                row.getColumn(SYSTABLES_COLUMN_SEQUENCE).getInt(),
                delimitedDVD != null ? delimitedDVD.getString() : null,
                escapedDVD != null ? escapedDVD.getString() : null,
                linesDVD != null ? linesDVD.getString() : null,
                storedDVD != null ? storedDVD.getString() : null,
                locationDVD != null ? locationDVD.getString() : null,
                compressionDVD != null ? compressionDVD.getString() : null,
                isPinnedDVD.getBoolean(),
                purgeDeletedRowsDVD.getBoolean(),
                minRetentionPeriodDVD != null ? minRetentionPeriodDVD.getLong() : null
        );
        tabDesc.setUUID(tableUUID);

        if (versionDescriptor != null) {
            tabDesc.setVersion(versionDescriptor.getString());
        } else
            tabDesc.setVersion(ORIGINAL_TABLE_VERSION);
        return tabDesc;
    }

    /**
     * Get the table name out of this SYSTABLES row
     *
     * @param row a SYSTABLES row
     * @throws StandardException thrown on failure
     * @return string, the table name
     */
    protected String getTableName(ExecRow row)
            throws StandardException {
        DataValueDescriptor col;

        col = row.getColumn(SYSTABLES_TABLENAME);
        return col.getString();
    }


    /**
     * builds a list of columns suitable for creating this catalog.
     *
     * @return array of systemcolumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList()
            throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getUUIDColumn(TABLEID, false),
                SystemColumnImpl.getIdentifierColumn(TABLENAME, false),
                SystemColumnImpl.getIndicatorColumn(TABLETYPE),
                SystemColumnImpl.getUUIDColumn(SCHEMAID, false),
                SystemColumnImpl.getIndicatorColumn(LOCKGRANULARITY),
                SystemColumnImpl.getIdentifierColumn(VERSION, true),
                SystemColumnImpl.getColumn(COLSEQUENCE, Types.INTEGER, false),
                SystemColumnImpl.getColumn(DELIMITED, Types.VARCHAR, true),
                SystemColumnImpl.getColumn(ESCAPED, Types.VARCHAR, true),
                SystemColumnImpl.getColumn(LINES, Types.VARCHAR, true),
                SystemColumnImpl.getColumn(STORED, Types.VARCHAR, true),
                SystemColumnImpl.getColumn(LOCATION, Types.VARCHAR, true),
                SystemColumnImpl.getColumn(COMPRESSION, Types.VARCHAR, true),
                SystemColumnImpl.getColumn(IS_PINNED, Types.BOOLEAN, false),
                SystemColumnImpl.getColumn(PURGE_DELETED_ROWS, Types.BOOLEAN, false),
                SystemColumnImpl.getColumn(MIN_RETENTION_PERIOD, Types.BIGINT, true),
        };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {
        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
                new ColumnDescriptor[]{
                        new ColumnDescriptor(TABLEID, 1, 1, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(TABLENAME, 2, 2,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(TABLETYPE, 3, 3,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(SCHEMAID, 4, 4,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(LOCKGRANULARITY, 5, 5,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(VERSION, 6, 6,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(COLSEQUENCE, 7, 7,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, false),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(DELIMITED, 8, 8,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(ESCAPED, 9, 9,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(LINES, 10, 10,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(STORED, 11, 11,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(LOCATION, 12, 12,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(COMPRESSION, 13, 13,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(IS_PINNED, 14, 14,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, false),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(PURGE_DELETED_ROWS, 15, 15,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, false),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor(MIN_RETENTION_PERIOD, 16, 16,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, true),
                                null, null, view, viewId, 0, 0, 0),
                        new ColumnDescriptor("SCHEMANAME", 17, 17,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null, null, view, viewId, 0, 0, 0)
                });

        // add columnlist for the systables view in sysibm schema
        Collection<Object[]> colList = Lists.newArrayListWithCapacity(66);
        colList.add(new Object[]{"NAME", Types.VARCHAR, false, 128});
        colList.add(new Object[]{"CREATOR", Types.VARCHAR, false, 128});
        colList.add(new Object[]{"TYPE", Types.CHAR, false, 1});
        colList.add(new Object[]{"COLCOUNT", Types.SMALLINT, false, null});
        colList.add(new Object[]{"KEYCOLUMNS", Types.SMALLINT, true, null});
        colList.add(new Object[]{"KEYUNIQUE", Types.SMALLINT, false, null});
        colList.add(new Object[]{"CODEPAGE", Types.SMALLINT, false, null});

        Collection<ColumnDescriptor> columnDescriptors = Lists.newArrayListWithCapacity(66);
        int colPos = 0;
        for (Object[] entry : colList) {
            colPos++;
            if (entry[3] != null) {
                columnDescriptors.add(new ColumnDescriptor((String) entry[0], colPos, colPos, DataTypeDescriptor.getBuiltInDataTypeDescriptor((int) entry[1], (boolean) entry[2], (int) entry[3]),
                        null, null, view, viewId, 0, 0, 0));
            } else {
                columnDescriptors.add(new ColumnDescriptor((String) entry[0], colPos, colPos, DataTypeDescriptor.getBuiltInDataTypeDescriptor((int) entry[1], (boolean) entry[2]),
                        null, null, view, viewId, 0, 0, 0));
            }
        }

        ColumnDescriptor[] arr = new ColumnDescriptor[columnDescriptors.size()];
        arr = columnDescriptors.toArray(arr);
        cdsl.add(arr);

        return cdsl;
    }

    public static final String SYSTABLE_VIEW_NAME = "SYSTABLESVIEW";
    public static final String SYSTABLE_VIEW_SQL = "CREATE VIEW " + SYSTABLE_VIEW_NAME + " AS \n" +
            "SELECT T.*, S.SCHEMANAME FROM SYS.SYSTABLES T, SYSVW.SYSSCHEMASVIEW S \n" +
            "WHERE T.SCHEMAID = S.SCHEMAID";

    public static final String SYSTABLE_VIEW_NAME_IN_SYSIBM = "systables";
    public static final String SYSTABLES_VIEW_IN_SYSIBM = "create view " + SYSTABLE_VIEW_NAME_IN_SYSIBM + " as \n" +
            "select\n" +
            "T.tablename as NAME,\n" +
            "T.schemaname as CREATOR,\n" +
            "case when T.tabletype='S' then 'T' else T.tabletype end as TYPE,\n" +
            "case when C.COLCOUNT is null then 0 else C.COLCOUNT end as COLCOUNT,\n" +
            "case when PKCOLS.CC is null then 0 else PKCOLS.CC end as KEYCOLUMNS,\n" +
            "case when KEYS.CC is null then 0 else KEYS.CC end as KEYUNIQUE,\n" +
            "case when T.tabletype='A' then 0 else 1208 end as CODEPAGE\n" +
            "from (\n" +
            "      select T.tablename, T.tableid, T.tabletype, S.schemaname\n" +
            "      from\n" +
            "      sys.systables T, sys.sysschemas S\n" +
            "      where T.schemaid=S.schemaid) T\n" +
            "left join -- compute number of columns in a table\n" +
            "(select C.referenceid, count(*) as COLCOUNT\n" +
            "      from sys.syscolumns C group by 1) C on T.tableid = C.referenceid\n" +
            "left join -- compute the number of PK columns\n" +
            "(select cols.referenceid as tableid,\n" +
            "        count(*) as CC\n" +
            " from sys.sysconstraints cons,\n" +
            "      sys.sysprimarykeys keys,\n" +
            "      sys.sysconglomerates congloms,\n" +
            "      sys.syscolumns cols\n" +
            " where  cols.referenceid = congloms.tableid and\n" +
            "        cons.tableid = cols.referenceid and cons.type = 'P' and\n" +
            "        cons.constraintid = keys.constraintid and\n" +
            "        (case when congloms.descriptor is not null then congloms.descriptor.getKeyColumnPosition(cols.columnnumber) else 0 end) <> 0 and\n" +
            "        keys.conglomerateid = congloms.conglomerateid\n" +
            " group by 1) PKCOLS on T.tableid = PKCOLS.tableid\n" +
            "left join -- compute unique constraint\n" +
            "(select C.tableid, count(*) as CC\n" +
            " from sys.sysconstraints C, sys.syskeys as K\n" +
            " where C.constraintid = K.constraintid and C.type <>'P'\n" +
            " group by 1) KEYS on T.tableid = KEYS.tableid";
}
