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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import java.sql.Types;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.UniqueTupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.compile.ColumnDefinitionNode;

/**
 * Factory for creating a SYSCOLUMNS row.
 *
 *
 * @version 0.1
 */

public class SYSCOLUMNSRowFactory extends CatalogRowFactory {
    static final String		TABLENAME_STRING = "SYSCOLUMNS";

    protected static final int		SYSCOLUMNS_COLUMN_COUNT = 11;
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

        colDesc = new ColumnDescriptor(columnName, columnNumber,storageNumber,
                dataTypeServices, defaultValue, defaultInfo, uuid,
                defaultUUID, autoincStart, autoincInc,
                autoincValue,collectStats, columnPosition!=null?columnPosition.getInt():-1);
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
                SystemColumnImpl.getColumn("PARTITIONPOSITION", Types.INTEGER, true)
        };
    }
}
