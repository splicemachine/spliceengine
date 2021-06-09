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

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.utils.Pair;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating a SYSCONGLOMERATES row.
 *
 */

public class SYSCONGLOMERATESRowFactory extends CatalogRowFactory
{
    private static final String        TABLENAME_STRING = "SYSCONGLOMERATES";

    protected static final int        SYSCONGLOMERATES_COLUMN_COUNT = 8;
    protected static final int        SYSCONGLOMERATES_SCHEMAID = 1;
    protected static final int        SYSCONGLOMERATES_TABLEID = 2;
    protected static final int        SYSCONGLOMERATES_CONGLOMERATENUMBER = 3;
    protected static final int        SYSCONGLOMERATES_CONGLOMERATENAME = 4;
    protected static final int        SYSCONGLOMERATES_ISINDEX = 5;
    protected static final int        SYSCONGLOMERATES_DESCRIPTOR = 6;
    protected static final int        SYSCONGLOMERATES_ISCONSTRAINT = 7;
    protected static final int        SYSCONGLOMERATES_CONGLOMERATEID = 8;

    protected static final int        SYSCONGLOMERATES_INDEX1_ID = 0;
    protected static final int        SYSCONGLOMERATES_INDEX2_ID = 1;
    protected static final int        SYSCONGLOMERATES_INDEX3_ID = 2;
    public static final int           SYSCONGLOMERATES_INDEX4_ID = 3;

    private    static    final    boolean[]    uniqueness = {
                                                       false,
                                                       true,
                                                       false,
                                                       false
                                                     };

    private static final int[][] indexColumnPositions =
    {
        {SYSCONGLOMERATES_CONGLOMERATEID},
        {SYSCONGLOMERATES_CONGLOMERATENAME, SYSCONGLOMERATES_SCHEMAID},
        {SYSCONGLOMERATES_TABLEID},
        {SYSCONGLOMERATES_CONGLOMERATENUMBER}
    };

    private    static    final    String[]    uuids =
    {
         "80000010-00d0-fd77-3ed8-000a0a0b1900"    // catalog UUID
        ,"80000027-00d0-fd77-3ed8-000a0a0b1900"    // heap UUID
        ,"80000012-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX1
        ,"80000014-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX2
        ,"80000016-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX3
        ,"80000017-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX4
    };

    SYSCONGLOMERATESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
    {
        super(uuidf,ef,dvf, dd);
        initInfo(SYSCONGLOMERATES_COLUMN_COUNT,
                 TABLENAME_STRING, indexColumnPositions,
                 uniqueness, uuids );
    }

  /**
     * Make a SYSCONGLOMERATES row
     *
     * @return    Row suitable for inserting into SYSCONGLOMERATES.
     *
     * @exception   StandardException thrown on failure
     */
    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent)
                    throws StandardException
    {
        ExecRow                    row;
        String                    tabID =null;
        Long                    conglomNumber = null;
        String                    conglomName = null;
        Boolean                    supportsIndex = null;
        IndexRowGenerator        indexRowGenerator = null;
        Boolean                    supportsConstraint = null;
        String                    conglomUUIDString = null;
        String                    schemaID = null;
        ConglomerateDescriptor  conglomerate = null;



        /* Insert info into sysconglomerates */

        if (td != null)
        {
            if (!(td instanceof ConglomerateDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + parent.getClass().getName());

            conglomerate = (ConglomerateDescriptor)td;

            /* Sometimes the SchemaDescriptor is non-null and sometimes it
             * is null.  (We can't just rely on getting the schema id from
             * the ConglomerateDescriptor because it can be null when
             * we are creating a new conglomerate.
             */
            if (parent != null)
            {
                if (!(parent instanceof SchemaDescriptor))
                    throw new RuntimeException("Unexpected TupleDescriptor " + parent.getClass().getName());
                SchemaDescriptor sd = (SchemaDescriptor)parent;
                schemaID = sd.getUUID().toString();
            }
            else
            {
                schemaID = conglomerate.getSchemaID().toString();
            }
            tabID = conglomerate.getTableID().toString();
            conglomNumber = conglomerate.getConglomerateNumber();
            conglomName = conglomerate.getConglomerateName();
            conglomUUIDString = conglomerate.getUUID().toString();

            supportsIndex = conglomerate.isIndex();
            indexRowGenerator = conglomerate.getIndexDescriptor();
            supportsConstraint = conglomerate.isConstraint();
        }

        /* RESOLVE - It would be nice to require less knowledge about sysconglomerates
         * and have this be more table driven.
         */

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSCONGLOMERATES_COLUMN_COUNT);

        /* 1st column is SCHEMAID (UUID - char(36)) */
        row.setColumn(1, new SQLChar(schemaID));

        /* 2nd column is TABLEID (UUID - char(36)) */
        row.setColumn(2, new SQLChar(tabID));

        /* 3rd column is CONGLOMERATENUMBER (long) */
        row.setColumn(3, new SQLLongint(conglomNumber));

        /* 4th column is CONGLOMERATENAME (varchar(128))
        ** If null, use the tableid so we always
        ** have a unique column
        */
        row.setColumn(4, (conglomName == null) ?
                new SQLVarchar(tabID): new SQLVarchar(conglomName));

        /* 5th  column is ISINDEX (boolean) */
        row.setColumn(5, new SQLBoolean(supportsIndex));

        /* 6th column is DESCRIPTOR
        *  (user type com.splicemachine.db.catalog.IndexDescriptor)
        */
        row.setColumn(6,
            new UserType(
                        (indexRowGenerator == null ?
                            (IndexDescriptor) null :
                            indexRowGenerator.getIndexDescriptor()
                        )
                    )
                );

        /* 7th column is ISCONSTRAINT (boolean) */
        row.setColumn(7, new SQLBoolean(supportsConstraint));

        /* 8th column is CONGLOMERATEID (UUID - char(36)) */
        row.setColumn(8, new SQLChar(conglomUUIDString));

        return row;
    }

    public ExecRow makeEmptyRow() throws StandardException
    {
        return makeRow(null, null);
    }

    ///////////////////////////////////////////////////////////////////////////
    //
    //    ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     *
     * @param row a SYSCOLUMNS row
     * @param parentTupleDescriptor    Null for this kind of descriptor.
     * @param dd dataDictionary
     *
     * @return    a conglomerate descriptor equivalent to a SYSCONGOMERATES row
     *
     * @exception   StandardException thrown on failure
     */

    public TupleDescriptor buildDescriptor(
        ExecRow                    row,
        TupleDescriptor            parentTupleDescriptor,
        DataDictionary             dd )
                    throws StandardException
    {
        if (SanityManager.DEBUG)
        SanityManager.ASSERT(
            row.nColumns() == SYSCONGLOMERATES_COLUMN_COUNT,
            "Wrong number of columns for a SYSCONGLOMERATES row");

        DataDescriptorGenerator    ddg = dd.getDataDescriptorGenerator();
        long conglomerateNumber;
        String    name;
        boolean isConstraint;
        boolean isIndex;
        IndexRowGenerator    indexRowGenerator;
        DataValueDescriptor col;
        ConglomerateDescriptor conglomerateDesc;
        String        conglomUUIDString;
        UUID        conglomUUID;
        String        schemaUUIDString;
        UUID        schemaUUID;
        String        tableUUIDString;
        UUID        tableUUID;

        /* 1st column is SCHEMAID (UUID - char(36)) */
        col = row.getColumn(1);
        schemaUUIDString = col.getString();
        schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);

        /* 2nd column is TABLEID (UUID - char(36)) */
        col = row.getColumn(2);
        tableUUIDString = col.getString();
        tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);


        /* 3nd column is CONGLOMERATENUMBER (long) */
        col = row.getColumn(3);
        conglomerateNumber = col.getLong();

        /* 4rd column is CONGLOMERATENAME (varchar(128)) */
        col = row.getColumn(4);
        name = col.getString();

        /* 5th column is ISINDEX (boolean) */
        col = row.getColumn(5);
        isIndex = col.getBoolean();

        /* 6th column is DESCRIPTOR */
        col = row.getColumn(6);
        indexRowGenerator = new IndexRowGenerator(
            (IndexDescriptor) col.getObject());

        /* 7th column is ISCONSTRAINT (boolean) */
        col = row.getColumn(7);
        isConstraint = col.getBoolean();

        /* 8th column is CONGLOMERATEID (UUID - char(36)) */
        col = row.getColumn(8);
        conglomUUIDString = col.getString();
        conglomUUID = getUUIDFactory().recreateUUID(conglomUUIDString);

        /* now build and return the descriptor */
        conglomerateDesc = ddg.newConglomerateDescriptor(conglomerateNumber,
                                                         name,
                                                         isIndex,
                                                         indexRowGenerator,
                                                         isConstraint,
                                                         conglomUUID,
                                                         tableUUID,
                                                         schemaUUID);
        return conglomerateDesc;
    }

    /**
     * Get the conglomerate's UUID of the row.
     *
     * @param row    The row from sysconglomerates
     *
     * @return UUID    The conglomerates UUID
     *
     * @exception   StandardException thrown on failure
     */
     protected UUID getConglomerateUUID(ExecRow row)
         throws StandardException
     {
        DataValueDescriptor    col;
        String                conglomerateUUIDString;

        /* 8th column is CONGLOMERATEID (UUID - char(36)) */
        col = row.getColumn(SYSCONGLOMERATES_CONGLOMERATEID);
        conglomerateUUIDString = col.getString();
        return getUUIDFactory().recreateUUID(conglomerateUUIDString);
     }

    /**
     * Get the table's UUID from the row.
     *
     * @param row    The row from sysconglomerates
     *
     * @return UUID    The table's UUID
     *
     * @exception   StandardException thrown on failure
     */
     protected UUID getTableUUID(ExecRow row)
         throws StandardException
     {
        DataValueDescriptor    col;
        String                tableUUIDString;

        /* 2nd column is TABLEID (UUID - char(36)) */
        col = row.getColumn(SYSCONGLOMERATES_TABLEID);
        tableUUIDString = col.getString();
        return getUUIDFactory().recreateUUID(tableUUIDString);
     }

    /**
     * Get the schema's UUID from the row.
     *
     * @param row    The row from sysconglomerates
     *
     * @return UUID    The schema's UUID
     *
     * @exception   StandardException thrown on failure
     */
     protected UUID getSchemaUUID(ExecRow row)
         throws StandardException
     {
        DataValueDescriptor    col;
        String                schemaUUIDString;

        /* 1st column is SCHEMAID (UUID - char(36)) */
        col = row.getColumn(SYSCONGLOMERATES_SCHEMAID);
        schemaUUIDString = col.getString();
        return getUUIDFactory().recreateUUID(schemaUUIDString);
     }

    /**
     * Get the conglomerate's name of the row.
     *
     * @param row    The row from sysconglomerates
     *
     * @return String    The conglomerates name
     *
     * @exception   StandardException thrown on failure
     */
     protected String getConglomerateName(ExecRow row)
         throws StandardException
     {
        DataValueDescriptor    col;

        /* 4th column is CONGLOMERATENAME (varchar(128)) */
        col = row.getColumn(SYSCONGLOMERATES_CONGLOMERATENAME);
        return col.getString();
     }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */

    public SystemColumn[]    buildColumnList()
        throws StandardException
    {
            return new SystemColumn[] {
               SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
               SystemColumnImpl.getUUIDColumn("TABLEID", false),
               SystemColumnImpl.getColumn("CONGLOMERATENUMBER", Types.BIGINT, false),
               SystemColumnImpl.getIdentifierColumn("CONGLOMERATENAME", true),
               SystemColumnImpl.getColumn("ISINDEX", Types.BOOLEAN, false),
               SystemColumnImpl.getJavaColumn("DESCRIPTOR",
                       "com.splicemachine.db.catalog.IndexDescriptor", true),
               SystemColumnImpl.getColumn("ISCONSTRAINT", Types.BOOLEAN, true),
               SystemColumnImpl.getUUIDColumn("CONGLOMERATEID", false)
           };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        assert cdsl.size() == SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL.getFirst();
        cdsl.add( getSYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL_ColumnDescriptor(view, viewId) );
        assert cdsl.size() == SYSCAT_INDEXCOLUSE_VIEW_SQL.getFirst();
        cdsl.add( getSYSCAT_INDEXCOLUSE_VIEW_SQL_ColumnDescriptor(view, viewId) );
        assert cdsl.size() == SYSIBM_SYSINDEXES_VIEW_SQL.getFirst();
        cdsl.add( getSYSIBM_SYSINDEXES_VIEW_SQL_ColumnDescriptor(view, viewId) );
        assert cdsl.size() == SYSVW_SYSCONGLOMERATES_SQL.getFirst();
        cdsl.add( getSYSVW_SYSCONGLOMERATES_SQL_ColumnDescriptor(view, viewId) );

        return cdsl;
    }

    final public static Pair<Integer, String> SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL = new Pair<>(0,
            "create view SYSCONGLOMERATEINSCHEMAS as \n" +
            "SELECT C.CONGLOMERATENUMBER, C.CONGLOMERATENAME, S.SCHEMANAME, T.TABLENAME, C.ISINDEX, " +
            "C.ISCONSTRAINT FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYSVW.SYSSCHEMASVIEW S "+
            "WHERE T.TABLEID = C.TABLEID AND T.SCHEMAID = S.SCHEMAID");


    private ColumnDescriptor[] getSYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL_ColumnDescriptor(TableDescriptor view, UUID viewId) {
        return new ColumnDescriptor[]{
                    getCD(view, viewId, "CONGLOMERATENUMBER", 1, Types.BIGINT, false),
                    getCD(view, viewId, "CONGLOMERATENAME",   2, Types.VARCHAR, true, 128),
                    getCD(view, viewId, "SCHEMANAME",         3, Types.VARCHAR, false, 128),
                    getCD(view, viewId, "TABLENAME",          4, Types.VARCHAR, false, 128),
                    getCD(view, viewId, "ISINDEX",            5, Types.BOOLEAN, false),
                    getCD(view, viewId, "ISCONSTRAINT",       6, Types.BOOLEAN, false)

            };
    }

    final public static Pair<Integer, String> SYSCAT_INDEXCOLUSE_VIEW_SQL = new Pair<>(1,
            "create view INDEXCOLUSE as \n" +
            "    SELECT  \n" +
            "           S.SCHEMANAME AS INDSCHEMA, \n" +
            "           CONGLOMS.CONGLOMERATENAME AS INDNAME, \n" +
            "           COLS.COLUMNNAME AS COLNAME, \n" +
            "           CAST (CONGLOMS.DESCRIPTOR.getKeyColumnPosition(COLS.COLUMNNUMBER) AS SMALLINT) AS COLSEQ, \n" +
            "           CASE WHEN CONGLOMS.DESCRIPTOR.isAscending( \n" +
            "                CONGLOMS.DESCRIPTOR.getKeyColumnPosition(COLS.COLUMNNUMBER)) THEN 'A' ELSE 'D' END AS COLORDER, \n" +
            "           CAST(NULL AS VARCHAR(128)) AS COLLATIONSCHEMA, \n" +
            "           CAST(NULL AS VARCHAR(128)) AS COLLATIONNAME, \n" +
            "           'N' AS VIRTUAL, \n" +
            "           CAST(NULL AS CLOB) AS TEXT \n" +
            "    FROM --splice-properties joinOrder=fixed \n" +
            "           SYSVW.SYSSCHEMASVIEW S --splice-properties useSpark=false \n" +
            "         , SYS.SYSCONGLOMERATES CONGLOMS --splice-properties index=SYSCONGLOMERATES_INDEX2, joinStrategy=nestedloop \n" +
            "         , SYS.SYSCOLUMNS COLS --splice-properties index=SYSCOLUMNS_INDEX1, joinStrategy=nestedloop \n" +
            "    WHERE \n" +
            "          CONGLOMS.SCHEMAID = S.SCHEMAID \n" +
            "      AND CASE WHEN CONGLOMS.DESCRIPTOR IS NULL THEN FALSE ELSE (CONGLOMS.DESCRIPTOR.getKeyColumnPosition(COLS.COLUMNNUMBER) > 0) END \n" +
            "      AND CONGLOMS.TABLEID = COLS.REFERENCEID \n" +
            "  UNION ALL \n" +
            "    SELECT \n" +
            "           S.SCHEMANAME AS INDSCHEMA, \n" +
            "           CONGLOMS.CONGLOMERATENAME AS INDNAME, \n" +
            "           CAST(NULL AS VARCHAR(128)) AS COLNAME, \n" +
            "           CAST (NUMBERS.N AS SMALLINT) AS COLSEQ, \n" +
            "           CASE WHEN CONGLOMS.DESCRIPTOR.isAscending(NUMBERS.N) THEN 'A' ELSE 'D' END AS COLORDER, \n" +
            "           CAST(NULL AS VARCHAR(128)) AS COLLATIONSCHEMA, \n" +
            "           CAST(NULL AS VARCHAR(128)) AS COLLATIONNAME, \n" +
            "           'S' AS VIRTUAL,\n" +
            "           CAST(TRIM(CONGLOMS.DESCRIPTOR.getExprText(NUMBERS.N)) AS CLOB) AS TEXT \n" +
            "    FROM --splice-properties joinOrder=fixed \n" +
            "           SYSVW.SYSSCHEMASVIEW S --splice-properties useSpark=false \n" +
            "         , SYS.SYSCONGLOMERATES CONGLOMS --splice-properties index=SYSCONGLOMERATES_INDEX2, joinStrategy=nestedloop \n" +
            "         , SYS.SYSNATURALNUMBERS NUMBERS \n" +
            "    WHERE \n" +
            "          CONGLOMS.SCHEMAID = S.SCHEMAID \n" +
            "      AND CONGLOMS.ISINDEX \n" +
            "      AND CONGLOMS.DESCRIPTOR IS NOT NULL \n" +
            "      AND CONGLOMS.DESCRIPTOR.isOnExpression() \n" +
            "      AND NUMBERS.N <= CONGLOMS.DESCRIPTOR.numberOfOrderedColumns() \n" +
            "    ORDER BY INDSCHEMA, INDNAME, COLSEQ");

    private ColumnDescriptor[] getSYSCAT_INDEXCOLUSE_VIEW_SQL_ColumnDescriptor(TableDescriptor view, UUID viewId) {
        return new ColumnDescriptor[]{
                    getCD(view, viewId, "INDSCHEMA",       1, Types.VARCHAR,  false, 128),
                    getCD(view, viewId, "INDNAME",         2, Types.VARCHAR,  false, 128),
                    getCD(view, viewId, "COLNAME",         3, Types.VARCHAR,  false, 128),
                    getCD(view, viewId, "COLSEQ",          4, Types.SMALLINT, false),
                    getCD(view, viewId, "COLORDER",        5, Types.CHAR,     false, 1),
                    getCD(view, viewId, "COLLATIONSCHEMA", 6, Types.VARCHAR,  true,  128),
                    getCD(view, viewId, "COLLATIONNAME",   7, Types.VARCHAR,  true,  128),
                    getCD(view, viewId, "VIRTUAL",         8, Types.CHAR,     true,  1),
                    getCD(view, viewId, "TEXT",            9, Types.CLOB,     true)
            };
    }

    // Reference:
    // https://www.ibm.com/support/knowledgecenter/SSEPEK_12.0.0/cattab/src/tpc/db2z_sysibmsysindexestable.html
    final public static Pair<Integer, String> SYSIBM_SYSINDEXES_VIEW_SQL = new Pair<>(2,
            "create view SYSINDEXES as \n" +
            "SELECT \n" +
            "  C.CONGLOMERATENAME AS NAME, \n" +
            "  S1.SCHEMANAME AS CREATOR, \n" +
            "  T.TABLENAME AS TBNAME, \n" +
            "  S2.SCHEMANAME AS TBCREATOR, \n" +
            "  (CASE WHEN C.DESCRIPTOR.isPrimaryKey() THEN 'P' \n" +
            "        WHEN C.DESCRIPTOR.isUnique() THEN \n" +
            "             (CASE WHEN U.TYPE IS NOT NULL THEN 'C' \n" +
            "                   ELSE 'U' END) \n" +
            "        ELSE 'D' END) AS UNIQUERULE, \n" +
            "  CAST(C.DESCRIPTOR.numberOfOrderedColumns() AS SMALLINT) AS COLCOUNT \n" +
            "FROM \n" +
            "  SYS.SYSCONGLOMERATES C \n" +
            "  JOIN \n" +
            "  SYS.SYSSCHEMAS S1 \n" +
            "  ON C.DESCRIPTOR IS NOT NULL AND \n" +
            "     C.SCHEMAID = S1.SCHEMAID \n" +
            "  JOIN \n" +
            "  SYS.SYSTABLES T \n" +
            "  ON C.TABLEID = T.TABLEID \n" +
            "  JOIN \n" +
            "  SYS.SYSSCHEMAS S2 \n" +
            "  ON T.SCHEMAID = S2.SCHEMAID \n" +
            "  LEFT JOIN \n" +
            "  (SELECT SC.TYPE, K.CONGLOMERATEID \n" +
            "     FROM SYS.SYSCONSTRAINTS SC, SYS.SYSKEYS K   -- get all unique constraint conglomerate IDs \n" +
            "     WHERE K.CONSTRAINTID = SC.CONSTRAINTID) U \n" +
            "  ON C.ISCONSTRAINT AND \n" +
            "     C.CONGLOMERATEID = U.CONGLOMERATEID");

    private ColumnDescriptor[] getSYSIBM_SYSINDEXES_VIEW_SQL_ColumnDescriptor(TableDescriptor view, UUID viewId) {
        return new ColumnDescriptor[]{
                    getCD(view, viewId, "NAME",       1, Types.VARCHAR,  true,  128),
                    getCD(view, viewId, "CREATOR",    2, Types.VARCHAR,  false, 128),
                    getCD(view, viewId, "TBNAME",     3, Types.VARCHAR,  false, 128),
                    getCD(view, viewId, "TBCREATOR",  4, Types.VARCHAR,  false, 128),
                    getCD(view, viewId, "UNIQUERULE", 5, Types.CHAR,     false, 1),
                    getCD(view, viewId, "COLCOUNT",   6, Types.SMALLINT, false)
            };
    }

    final public static int SYSVW_SYSCONGLOMERATES_INDEX = 3;
    final public static Pair<Integer, String> SYSVW_SYSCONGLOMERATES_SQL = new Pair<>(3,
                    "create view SYSCONGLOMERATESVIEW as SELECT " +
                    "SCHEMAID, " +
                    "TABLEID, " +
                    "CONGLOMERATENUMBER, " +
                    "CONGLOMERATENAME, " +
                    "ISINDEX, " +
                    "cast(DESCRIPTOR as char(64)) as DESCRIPTOR, " +
                    "ISCONSTRAINT, " +
                    "CONGLOMERATEID FROM sys.SYSCONGLOMERATES");

    private ColumnDescriptor[] getSYSVW_SYSCONGLOMERATES_SQL_ColumnDescriptor(TableDescriptor view, UUID viewId) {
        return new ColumnDescriptor[]{
                getCD(view, viewId, "SCHEMAID",           1, Types.CHAR,    false, 36),
                getCD(view, viewId, "TABLEID",            2, Types.CHAR,    false, 36),
                getCD(view, viewId, "CONGLOMERATENUMBER", 3, Types.BIGINT,  false),
                getCD(view, viewId, "CONGLOMERATENAME",   4, Types.VARCHAR, true,  128),
                getCD(view, viewId, "ISINDEX",            5, Types.BOOLEAN, false),
                getCD(view, viewId, "DESCRIPTOR",         6, Types.CHAR,    true,  64),
                getCD(view, viewId, "ISCONSTRAINT",       7, Types.BOOLEAN, true),
                getCD(view, viewId, "CONGLOMERATEID",     8, Types.CHAR,    false, 36)
        };
    }
}
