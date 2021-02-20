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

    private    static    final    boolean[]    uniqueness = {
                                                       false,
                                                       true,
                                                       false
                                                     };

    private static final int[][] indexColumnPositions =
    {
        {SYSCONGLOMERATES_CONGLOMERATEID},
        {SYSCONGLOMERATES_CONGLOMERATENAME, SYSCONGLOMERATES_SCHEMAID},
        {SYSCONGLOMERATES_TABLEID}
    };

    private    static    final    String[]    uuids =
    {
         "80000010-00d0-fd77-3ed8-000a0a0b1900"    // catalog UUID
        ,"80000027-00d0-fd77-3ed8-000a0a0b1900"    // heap UUID
        ,"80000012-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX1
        ,"80000014-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX2
        ,"80000016-00d0-fd77-3ed8-000a0a0b1900"    // SYSCONGLOMERATES_INDEX3
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
        cdsl.add(
            new ColumnDescriptor[]{
                new ColumnDescriptor("CONGLOMERATENUMBER",1,1,DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("CONGLOMERATENAME",2,2,
                            DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 128),
                            null,null,view,viewId,0,0,0),
                new ColumnDescriptor("SCHEMANAME"               ,3,3,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("TABLENAME"               ,4,4,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("ISINDEX"               ,5,5,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("ISCONSTRAINT"               ,6,6,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, false),
                        null,null,view,viewId,0,0,0)

        });

        cdsl.add(
            new ColumnDescriptor[]{
                new ColumnDescriptor("INDSCHEMA",1,1,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("INDNAME",2,2,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLNAME",3,3,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLSEQ",4,4,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLORDER",5,5,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLLATIONSCHEMA",6,6,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLLATIONNAME",7,7,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("VIRTUAL",8,8,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, true, 1),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("TEXT",9,9,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CLOB, true),
                        null,null,view,viewId,0,0,0)
        });

        // SYSIBM.SYSINDEXES
        cdsl.add(
            new ColumnDescriptor[]{
                new ColumnDescriptor("NAME",1,1,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 128), // NOT NULL in DB2
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("CREATOR",2,2,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("TBNAME",3,3,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("TBCREATOR",4,4,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("UNIQUERULE",5,5,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLCOUNT",6,6,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT, false),
                        null,null,view,viewId,0,0,0)
            });

        return cdsl;
    }
    public static String SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL = "create view SYSCONGLOMERATEINSCHEMAS as \n" +
            "SELECT C.CONGLOMERATENUMBER, C.CONGLOMERATENAME, S.SCHEMANAME, T.TABLENAME, C.ISINDEX, C.ISCONSTRAINT FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYSVW.SYSSCHEMASVIEW S "+
            "WHERE T.TABLEID = C.TABLEID AND T.SCHEMAID = S.SCHEMAID";


    public static String SYSCAT_INDEXCOLUSE_VIEW_SQL = "create view INDEXCOLUSE as \n" +
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
            "    ORDER BY INDSCHEMA, INDNAME, COLSEQ";

    // Reference:
    // https://www.ibm.com/support/knowledgecenter/SSEPEK_12.0.0/cattab/src/tpc/db2z_sysibmsysindexestable.html
    public static String SYSIBM_SYSINDEXES_VIEW_SQL = "create view SYSINDEXES as \n" +
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
            "     C.CONGLOMERATEID = U.CONGLOMERATEID";
}
