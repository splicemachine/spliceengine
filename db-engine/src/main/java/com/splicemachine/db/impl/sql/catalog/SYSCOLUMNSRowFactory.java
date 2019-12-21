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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.compile.ColumnDefinitionNode;
import org.spark_project.guava.collect.Lists;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Factory for creating a SYSCOLUMNS row.
 *
 *
 * @version 0.1
 */

public class SYSCOLUMNSRowFactory extends CatalogRowFactory {
    public static final String		TABLENAME_STRING = "SYSCOLUMNS";

    protected static final int		SYSCOLUMNS_COLUMN_COUNT = 13;
	/* Column #s for syscolumns (1 based) */

    //TABLEID is an obsolete name, it is better to use
    //REFERENCEID, but to make life easier you can use either
    protected static final int		SYSCOLUMNS_TABLEID = 1;
    protected static final int		SYSCOLUMNS_REFERENCEID = 1;
    protected static final int		SYSCOLUMNS_COLUMNNAME = 2;
    protected static final int		SYSCOLUMNS_COLUMNNUMBER = 3;
    protected static final int		SYSCOLUMNS_STORAGECOLUMNNUMBER = 4;
    protected static final int		SYSCOLUMNS_COLUMNDATATYPE = 5;
    protected static final int		SYSCOLUMNS_COLUMNDEFAULT = 6;
    protected static final int		SYSCOLUMNS_COLUMNDEFAULTID = 7;
    protected static final int 		SYSCOLUMNS_AUTOINCREMENTVALUE = 8;
    protected static final int 		SYSCOLUMNS_AUTOINCREMENTSTART = 9;
    protected static final int		SYSCOLUMNS_AUTOINCREMENTINC = 10;
    protected static final int		SYSCOLUMNS_COLLECTSTATS = 11;
    protected static final int		SYSCOLUMNS_PARTITION_POSITION = 12;
    public static final int         SYSCOLUMNS_USEEXTRAPOLATION = 13;


    protected static final int		SYSCOLUMNS_INDEX1_ID = 0;
    protected static final int		SYSCOLUMNS_INDEX2_ID = 1;

    private	static	final	boolean[]	uniqueness = {
            true,
            false
    };

    private	static	final	String[]	uuids =
            {
                    "8000001e-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
                    ,"80000029-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
                    ,"80000020-00d0-fd77-3ed8-000a0a0b1900"	// SYSCOLUMNS_INDEX1 UUID
                    ,"6839c016-00d9-2829-dfcd-000a0a411400"	// SYSCOLUMNS_INDEX2 UUID
            };

    private static final int[][] indexColumnPositions =
            {
                    {SYSCOLUMNS_REFERENCEID, SYSCOLUMNS_COLUMNNAME},
                    {SYSCOLUMNS_COLUMNDEFAULTID}
            };

    /////////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    /////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    SYSCOLUMNSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
    {
        this(uuidf, ef, dvf, TABLENAME_STRING);
    }

    SYSCOLUMNSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                         String myName )
    {
        super(uuidf,ef,dvf);
        initInfo(SYSCOLUMNS_COLUMN_COUNT, myName, indexColumnPositions, uniqueness, uuids);
    }

    /////////////////////////////////////////////////////////////////////////////
    //
    //	METHODS
    //
    /////////////////////////////////////////////////////////////////////////////

    /**
     * Make a SYSCOLUMNS row
     *
     * @return	Row suitable for inserting into SYSCOLUMNS.
     *
     * @exception   StandardException thrown on failure
     */

    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException{
        ExecRow    				row;

        String					colName = null;
        String					defaultID = null;
        String					tabID = null;
        Integer					colID = null;
        Integer                 storageNumber = null;
        TypeDescriptor 		    typeDesc = null;
        Object					defaultSerializable = null;
        long					autoincStart = 0;
        long					autoincInc = 0;
        long					autoincValue = 0;
        int                     partitionPosition = -1;
        //The SYSCOLUMNS table's autoinc related columns change with different
        //values depending on what happened to the autoinc column, ie is the
        //user adding an autoincrement column, or is user changing the existing
        //autoincrement column to change it's increment value or to change it's
        //start value? Following variable is used to keep track of what happened
        //to the autoincrement column.
        long autoinc_create_or_modify_Start_Increment = -1;
        boolean collectStats = false;

        //add useExtrapolation as a byte int instead of boolean to accommodate for future extension
        // currently, value 0 means extraploation is not allowed, non-zero means extrapolation is allowed
        byte    useExtrapolation = 0;

        if (td != null) {
            ColumnDescriptor  column = (ColumnDescriptor)td;
		
			      /* Lots of info in the column's type descriptor */
            typeDesc = column.getType().getCatalogType();

            tabID = column.getReferencingUUID().toString();
            colName = column.getColumnName();
            colID = column.getPosition();
            storageNumber = column.getStoragePosition();
            autoincStart = column.getAutoincStart();
            autoincInc   = column.getAutoincInc();
            autoincValue   = column.getAutoincValue();
            autoinc_create_or_modify_Start_Increment = column.getAutoinc_create_or_modify_Start_Increment();
            if(column.getDefaultInfo() != null) {
                defaultSerializable = column.getDefaultInfo();
            }else{
                defaultSerializable = column.getDefaultValue();
            }

            if(column.getDefaultUUID() != null) {
                defaultID = column.getDefaultUUID().toString();
            }
            collectStats = column.collectStatistics();
            partitionPosition = column.getPartitionPosition();
            useExtrapolation = column.getUseExtrapolation();
        }

		/* Insert info into syscolumns */

		/* RESOLVE - It would be nice to require less knowledge about syscolumns
		 * and have this be more table driven.
		 * RESOLVE - We'd like to store the DataTypeDescriptor in a column.
		 */

		    /* Build the row to insert  */
        row = getExecutionFactory().getValueRow(SYSCOLUMNS_COLUMN_COUNT);

		    /* 1st column is REFERENCEID (UUID - char(36)) */
        row.setColumn(SYSCOLUMNS_REFERENCEID, new SQLChar(tabID));

		    /* 2nd column is COLUMNNAME (varchar(128)) */
        row.setColumn(SYSCOLUMNS_COLUMNNAME, new SQLVarchar(colName));

		    /* 3rd column is COLUMNNUMBER (int) */
        row.setColumn(SYSCOLUMNS_COLUMNNUMBER, new SQLInteger(colID));

        row.setColumn(SYSCOLUMNS_STORAGECOLUMNNUMBER, new SQLInteger(storageNumber));

		    /* 4th column is COLUMNDATATYPE */
        row.setColumn(SYSCOLUMNS_COLUMNDATATYPE,
                new UserType(typeDesc));

		    /* 5th column is COLUMNDEFAULT */
        row.setColumn(SYSCOLUMNS_COLUMNDEFAULT,
                new UserType(defaultSerializable));

		    /* 6th column is DEFAULTID (UUID - char(36)) */
        row.setColumn(SYSCOLUMNS_COLUMNDEFAULTID, new SQLChar(defaultID));

        if (autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.CREATE_AUTOINCREMENT ||
                autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE) {
        //user is adding an autoinc column or is changing the increment value of autoinc column
            // This code also gets run when ALTER TABLE DROP COLUMN
            // is used to drop a column other than the autoinc
            // column, and the autoinc column gets removed from
            // SYSCOLUMNS and immediately re-added with a different
            // column position (to account for the dropped column).
            // In this case, the autoincValue may have a
            // different value than the autoincStart.
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE,
                    new SQLLongint(autoincValue));
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART,
                    new SQLLongint(autoincStart));
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC,
                    new SQLLongint(autoincInc));
        } else if (autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE){
        //user asked for restart with a new value, so don't change increment by and original start
            //with values in the SYSCOLUMNS table. Just record the RESTART WITH value as the
            //next value to be generated in the SYSCOLUMNS table
            ColumnDescriptor  column = (ColumnDescriptor)td;
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, new SQLLongint(autoincStart));
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, new SQLLongint(autoincStart));
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC, new SQLLongint(autoincInc));
//                    column.getTableDescriptor().getColumnDescriptor(colName).getAutoincInc()));
        } else {
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, new SQLLongint());
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, new SQLLongint());
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC, new SQLLongint());
        }
        row.setColumn(SYSCOLUMNS_COLLECTSTATS,new SQLBoolean(collectStats));
        row.setColumn(SYSCOLUMNS_PARTITION_POSITION,new SQLInteger(partitionPosition));
        row.setColumn(SYSCOLUMNS_USEEXTRAPOLATION, new SQLTinyint(useExtrapolation));
        return row;
    }

    ///////////////////////////////////////////////////////////////////////////
    //
    //	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make a ColumnDescriptor out of a SYSCOLUMNS row
     *
     * @param row 					a SYSCOLUMNS row
     * @param parentTupleDescriptor	The UniqueTupleDescriptor for the object that is tied
     *								to this column
     * @param dd 					dataDictionary
     *
     * @return	a column descriptor equivalent to a SYSCOLUMNS row
     *
     * @exception   StandardException thrown on failure
     */
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTupleDescriptor,
                                           DataDictionary	dd) throws StandardException{
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(row.nColumns() == SYSCOLUMNS_COLUMN_COUNT,
                    "Wrong number of columns for a SYSCOLUMNS row");
        }

        int columnNumber;
        int storageNumber;
        String columnName;
        String defaultID;
        DefaultInfoImpl		defaultInfo = null;
        ColumnDescriptor colDesc;
        DataValueDescriptor	defaultValue = null;
        UUID				defaultUUID = null;
        UUID				uuid;
        UUIDFactory			uuidFactory = getUUIDFactory();
        long autoincStart, autoincInc, autoincValue;

        DataDescriptorGenerator	ddg = dd.getDataDescriptorGenerator();


		    /*
		     ** We're going to be getting the UUID for this sucka
		     ** so make sure it is a UniqueTupleDescriptor.
		     */
        if (parentTupleDescriptor != null) {
            if (SanityManager.DEBUG) {
                if (!(parentTupleDescriptor instanceof UniqueTupleDescriptor)) {
                    SanityManager.THROWASSERT(parentTupleDescriptor.getClass().getName()
                            + " not instanceof UniqueTupleDescriptor");
                }
            }
            uuid = ((UniqueTupleDescriptor)parentTupleDescriptor).getUUID();
        } else {
			      /* 1st column is REFERENCEID (char(36)) */
            uuid = uuidFactory.recreateUUID(row.getColumn(SYSCOLUMNS_REFERENCEID). getString());
        }

		    /* NOTE: We get columns 5 and 6 next in order to work around
		     * a 1.3.0 HotSpot bug.  (#4361550)
		     */
        // 5th column is COLUMNDEFAULT (serialiazable)
        Object object = row.getColumn(SYSCOLUMNS_COLUMNDEFAULT).getObject();
        if (object instanceof DataValueDescriptor) {
            defaultValue = (DataValueDescriptor) object;
        } else if (object instanceof DefaultInfoImpl) {
            defaultInfo = (DefaultInfoImpl) object;
            defaultValue = defaultInfo.getDefaultValue();
        }

		    /* 6th column is DEFAULTID (char(36)) */
        defaultID = row.getColumn(SYSCOLUMNS_COLUMNDEFAULTID).getString();

        if (defaultID != null) {
            defaultUUID = uuidFactory.recreateUUID(defaultID);
        }

		    /* 2nd column is COLUMNNAME (varchar(128)) */
        columnName = row.getColumn(SYSCOLUMNS_COLUMNNAME).getString();

		    /* 3rd column is COLUMNNUMBER (int) */
        columnNumber = row.getColumn(SYSCOLUMNS_COLUMNNUMBER).getInt();

        storageNumber= row.getColumn(SYSCOLUMNS_STORAGECOLUMNNUMBER).getInt();



		    /* 4th column is COLUMNDATATYPE */

		    /*
		     ** What is stored in the column is a TypeDescriptorImpl, which
		     ** points to a BaseTypeIdImpl.  These are simple types that are
		     ** intended to be movable to the client, so they don't have
		     ** the entire implementation.  We need to wrap them in DataTypeServices
		     ** and TypeId objects that contain the full implementations for
		     ** language processing.
		     */
        TypeDescriptor catalogType = (TypeDescriptor) row.getColumn(SYSCOLUMNS_COLUMNDATATYPE). getObject();
        DataTypeDescriptor dataTypeServices = DataTypeDescriptor.getType(catalogType);

		    /* 7th column is AUTOINCREMENTVALUE (long) */
        autoincValue = row.getColumn(SYSCOLUMNS_AUTOINCREMENTVALUE).getLong();

		    /* 8th column is AUTOINCREMENTSTART (long) */
        autoincStart = row.getColumn(SYSCOLUMNS_AUTOINCREMENTSTART).getLong();

		    /* 9th column is AUTOINCREMENTINC (long) */
        autoincInc = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINC).getLong();

        DataValueDescriptor col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTSTART);
        autoincStart = col.getLong();

        col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINC);
        autoincInc = col.getLong();

        /* 10th column is COLLECTSTATS */
        /*
         * For backwards compatibility, we know that the COLLECT_STATS column doesn't exist
         * for some databases, so when we describe the data, we have to ensure that twe deal
         * with that case by adding a proper default value.
         *
         * As of version 1.1, we do *not* collect statistics for non-keyed columns by default, so
         * we treat this as false if we can't find it. Later versions may default collection to true
         * for orderable types and/or primary keys, but for now we'll keep it simple.
         */
        DataValueDescriptor collectStatsColumn = row.getColumn(SYSCOLUMNS_COLLECTSTATS);
        boolean collectStats = false;
        if(collectStatsColumn!=null && !collectStatsColumn.isNull())
            collectStats = collectStatsColumn.getBoolean();

        DataValueDescriptor columnPosition = row.getColumn(SYSCOLUMNS_PARTITION_POSITION);

        DataValueDescriptor useExtrapolationColumn = row.getColumn(SYSCOLUMNS_USEEXTRAPOLATION);
        byte useExtrapolation = 0;
        if (useExtrapolationColumn != null && !useExtrapolationColumn.isNull())
            useExtrapolation = useExtrapolationColumn.getByte();

        colDesc = new ColumnDescriptor(columnName, columnNumber,storageNumber,
                dataTypeServices, defaultValue, defaultInfo, uuid,
                defaultUUID, autoincStart, autoincInc,
                autoincValue,collectStats, columnPosition!=null?columnPosition.getInt():-1,
                useExtrapolation);
        return colDesc;
    }

    /**
     *	Get the index number for the primary key index on this catalog.
     *
     *	@return	a 0-based number
     *
     */
    public	int	getPrimaryKeyIndexNumber() {
        return SYSCOLUMNS_INDEX1_ID;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[]	buildColumnList() throws StandardException {

        return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("REFERENCEID", false),
                SystemColumnImpl.getIdentifierColumn("COLUMNNAME", false),
                SystemColumnImpl.getColumn("COLUMNNUMBER", Types.INTEGER, false),
                SystemColumnImpl.getColumn("STORAGENUMBER", Types.INTEGER, false),
                SystemColumnImpl.getJavaColumn("COLUMNDATATYPE",
                        "com.splicemachine.db.catalog.TypeDescriptor", false),
                SystemColumnImpl.getJavaColumn("COLUMNDEFAULT",
                        "java.io.Serializable", true),
                SystemColumnImpl.getUUIDColumn("COLUMNDEFAULTID", true),
                SystemColumnImpl.getColumn("AUTOINCREMENTVALUE", Types.BIGINT, true),
                SystemColumnImpl.getColumn("AUTOINCREMENTSTART", Types.BIGINT, true),
                SystemColumnImpl.getColumn("AUTOINCREMENTINC", Types.BIGINT, true),
                SystemColumnImpl.getColumn("COLLECTSTATS", Types.BOOLEAN, true),
                SystemColumnImpl.getColumn("PARTITIONPOSITION", Types.INTEGER, true),
                SystemColumnImpl.getColumn("USEEXTRAPOLATION", Types.TINYINT, true)
        };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {
        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
            new ColumnDescriptor[]{
                new ColumnDescriptor("REFERENCEID",1,1,DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLUMNNAME"               ,2,2,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLUMNNUMBER"               ,3,3,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("STORAGENUMBER"               ,4,4,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLUMNDATATYPE"               ,5,5,
                        new DataTypeDescriptor(TypeId.getUserDefinedTypeId("com.splicemachine.db.catalog.TypeDescriptor", false), false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLUMNDEFAULT"               ,6,6,
                        new DataTypeDescriptor(TypeId.getUserDefinedTypeId("java.io.Serializable", true), false),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLUMNDEFAULTID"               ,7,7,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("AUTOINCREMENTVALUE"               ,8,8,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, true),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("AUTOINCREMENTSTART"               ,9,9,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, true),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("AUTOINCREMENTINC"               ,10,10,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, true),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("COLLECTSTATS"               ,11,11,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN, true),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("PARTITIONPOSITION"               ,12,12,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, true),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("USEEXTRAPOLATION"               ,13,13,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TINYINT, true),
                        null,null,view,viewId,0,0,0),
                new ColumnDescriptor("TABLENAME"               ,14,14,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                  null,null,view,viewId,0,0,0),
                new ColumnDescriptor("SCHEMANAME"               ,15,15,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                        null,null,view,viewId,0,0,0)
        });

        // add columnlist for the syscolumns view in sysibm schema
        Collection<Object[]> colList = Lists.newArrayListWithCapacity(50);
        colList.add(new Object[]{"NAME", Types.VARCHAR, false, 128});
        colList.add(new Object[]{"TBNAME", Types.VARCHAR, false, 128});
        colList.add(new Object[]{"TBCREATOR", Types.VARCHAR, false, 128});
        colList.add(new Object[]{"COLTYPE", Types.CHAR, false, 8});
        colList.add(new Object[]{"NULLS", Types.CHAR, false, 1});
        colList.add(new Object[]{"CODEPAGE", Types.SMALLINT, false, null});
        colList.add(new Object[]{"LENGTH", Types.SMALLINT, false, null});
        colList.add(new Object[]{"SCALE", Types.SMALLINT, false, null});
        colList.add(new Object[]{"COLNO", Types.SMALLINT, false, null});
        colList.add(new Object[]{"TYPENAME", Types.VARCHAR, false, 128});
        colList.add(new Object[]{"LONGLENGTH", Types.INTEGER, false, null});
        colList.add(new Object[]{"KEYSEQ", Types.SMALLINT, true, null});


        Collection<ColumnDescriptor> columnDescriptors = Lists.newArrayListWithCapacity(50);
        int colPos = 0;
        for (Object[] entry: colList) {
            colPos ++;
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
    public static String SYSCOLUMNS_VIEW_SQL = "create view SYSCOLUMNSVIEW as \n" +
            "SELECT C.*, " +
            "T.TABLENAME, " +
            "T.SCHEMANAME " +
            "FROM SYS.SYSCOLUMNS C, SYSVW.SYSTABLESVIEW T WHERE T.TABLEID = C.REFERENCEID";


    public static String SYSCOLUMNS_VIEW_IN_SYSIBM = "create view SYSCOLUMNS as \n" +
            "select\n" +
            "COL.columnname as NAME,\n" +
            "COL.tablename as TBNAME,\n" +
            "COL.schemaname as TBCREATOR,\n" +
            "cast (case when COL.COLUMNTYPE='TIMESTAMP' then 'TIMESTMP'\n" +
            "     when COL.COLUMNTYPE='VARBINARY' then 'VARBIN'\n" +
            "     when COL.COLUMNTYPE='LONG VARCHAR' then 'LONGVAR'\n" +
            "     when COL.COLUMNTYPE like 'CHAR%' then 'CHAR'\n" +
            "     when COL.COLUMNTYPE like 'VARCHAR%' then 'VARCHAR'\n" +
            "     when COL.COLUMNTYPE like 'com.%' or COL.COLUMNTYPE like 'java.%' or COL.COLUMNTYPE like 'org.%' then 'DISTINCT'\n" +
            "     when length(COL.COLUMNTYPE) > 8 then substr(COL.COLUMNTYPE, 1, 8)\n" +
            "     else COL.COLUMNTYPE end as CHAR(8)) as COLTYPE,\n" +
            "case when COL.COLUMNDATATYPE.isNullable() then 'Y' else 'N' end NULLS,\n" +
            "case when COL.COLUMNTYPE in ('CHAR', 'VARCHAR', 'CLOB') then 1208 else 0 end as CODEPAGE,\n" +
            "case when COL.COLUMNTYPE='INTEGER' then 4\n" +
            "     when COL.COLUMNTYPE='SMALLINT' then 2\n" +
            "     when COL.COLUMNTYPE='BIGINT' then 8\n" +
            "     when COL.COLUMNTYPE='FLOAT' then 4\n" +
            "     when COL.COLUMNTYPE='DATE' then 4\n" +
            "     when COL.COLUMNTYPE='TIMESTAMP' then 10\n" +
            "     when COL.COLUMNTYPE='TIME' then 3\n" +
            "     when COL.COLUMNTYPE='DECIMAL' then COL.COLUMNDATATYPE.getPrecision()\n" +
            "     when COL.COLUMNDATATYPE.getMaximumWidth() > 32767 then -1\n" +
            "     else COL.COLUMNDATATYPE.getMaximumWidth() end as LENGTH,\n" +
            "case when COL.COLUMNTYPE='DECIMAL' then COL.COLUMNDATATYPE.getScale()\n" +
            "     when COL.COLUMNTYPE='TIMESTAMP' then 6\n" +
            "     else 0 end as SCALE,\n" +
            "COL.columnnumber-1 as COLNO, -- 0-based\n" +
            "case when COL.COLUMNTYPE='CHAR' then 'CHARACTER'\n" +
            "     else COL.COLUMNTYPE end as TYPENAME,\n" +
            "case when COL.COLUMNTYPE='INTEGER' then 4\n" +
            "     when COL.COLUMNTYPE='SMALLINT' then 2\n" +
            "     when COL.COLUMNTYPE='BIGINT' then 8\n" +
            "     when COL.COLUMNTYPE='FLOAT' then 4\n" +
            "     when COL.COLUMNTYPE='DATE' then 4\n" +
            "     when COL.COLUMNTYPE='TIMESTAMP' then 10\n" +
            "     when COL.COLUMNTYPE='TIME' then 3\n" +
            "     when COL.COLUMNTYPE='DECIMAL' then COL.COLUMNDATATYPE.getPrecision()\n" +
            "     else COL.COLUMNDATATYPE.getMaximumWidth() end as LONGLENGTH,\n" +
            "case when CON.keydesc is not null and CON.keydesc.getKeyColumnPosition(COL.columnnumber) > 0 then CON.keydesc.getKeyColumnPosition(COL.columnnumber)\n" +
            "     end as KEYSEQ\n" +
            "from \n" +
            "(select c.columnname,\n" +
            "        t.tablename,\n" +
            "        s.schemaname,\n" +
            "        c.columnnumber,\n" +
            "        c.columndatatype,\n" +
            "        cast (c.columndatatype.getTypeName() as varchar(128)) as columntype,\n" +
            "        t.tableid\n" +
            "from sys.syscolumns c,\n" +
            "     sys.systables t,\n" +
            "     sys.sysschemas s\n" +
            "where c.referenceid = t.tableid and\n" +
            "      t.schemaid = s.schemaid\n" +
            ") col\n" +
            "left join (\n" +
            "select cons.tableid,\n" +
            "       congloms.descriptor as keydesc\n" +
            "from sys.sysconstraints cons,\n" +
            "     sys.sysprimarykeys keys,\n" +
            "     sys.sysconglomerates congloms\n" +
            "where cons.type = 'P' and\n" +
            "      cons.constraintid = keys.constraintid and\n" +
            "      cons.tableid = congloms.tableid and\n" +
            "      keys.conglomerateid = congloms.conglomerateid\n" +
            ") con on col.tableid = con.tableid";
}
