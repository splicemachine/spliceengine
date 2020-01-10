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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.catalog.DefaultInfo;
import com.splicemachine.db.catalog.UUID;

/**
 * This class represents a column descriptor.
 *
 * public methods in this class are:
 * <ol>
 * <li>long getAutoincStart()</li>
 * <li>java.lang.String getColumnName()</li>
 * <li>DefaultDescriptor getDefaultDescriptor(DataDictionary dd)</li>
 * <li>DefaultInfo getDefaultInfo</li>
 * <li>UUID getDefaultUUID</li>
 * <li>DataValueDescriptor getDefaultValue</li>
 * <li>int getPosition()</li>
 * <li>UUID getReferencingUUID()</li>
 * <li>TableDescriptor getTableDescriptor</li>
 * <li>DTD getType()</li>
 * <li>hasNonNullDefault</li>
 * <li>isAutoincrement</li>
 * <li>setColumnName</li>
 * <li>setPosition</li>
 * <li>useExtrapolation</li>
 *</ol>
 */

public final class ColumnDescriptor extends TupleDescriptor
{

    // implementation
    private DefaultInfo			columnDefaultInfo;
    private TableDescriptor		table;
    private String			columnName;
    private int			columnPosition;
    private int         storagePosition;
    private DataTypeDescriptor	columnType;
    private DataValueDescriptor	columnDefault;
    private UUID				uuid;
    private UUID				defaultUUID;
    private long				autoincStart;
    private long				autoincInc;
    private long				autoincValue;
    private boolean collectStatistics;
    private int partitionPosition = -1;
    private byte  useExtrapolation = 0;
    /* Used for Serde */
    //Following variable is used to see if the user is adding an autoincrement
    //column, or if user is altering the existing autoincrement column to change
    //the increment value or to change the start value. If none of the above,
    //then it will be set to -1
    long				autoinc_create_or_modify_Start_Increment = -1;

    /**
     * Constructor for a ColumnDescriptor when the column involved
     * is an autoincrement column. The last parameter to this method
     * indicates if an autoincrement column is getting added or if
     * the autoincrement column is being modified to change the
     * increment value or to change the start value
     *
     * @param columnName		The name of the column
     * @param columnPosition	The ordinal position of the column in the table
     * @param storagePosition	The ordinal position of the column on disk (Accounts for alter table)
     *
     * @param columnType		A DataTypeDescriptor for the type of
     *				the column
     * @param columnDefault		A DataValueDescriptor representing the
     *							default value of the column, if any
     *							(null if no default)
     * @param columnDefaultInfo		The default info for the column.
     * @param table			A TableDescriptor for the table the
     *						column is in
     * @param defaultUUID			The UUID for the default, if any.
     * @param autoincStart	Start value for an autoincrement column.
     * @param autoincInc	Increment for autoincrement column
     * @param userChangedWhat		Adding an autoincrement column OR
     *						changing increment value or start value of
     *						the autoincrement column.
     * @param partitionPosition
     * @param useExtrapolation indicates whether we want to do extrapolation for stats estimation
     */

    public ColumnDescriptor(String columnName, int columnPosition, int storagePosition,
                            DataTypeDescriptor columnType, DataValueDescriptor columnDefault,
                            DefaultInfo columnDefaultInfo,
                            TableDescriptor table,
                            UUID defaultUUID, long autoincStart, long autoincInc,
                            long userChangedWhat,
                            int partitionPosition,
                            byte useExtrapolation)
    {
        this(columnName, columnPosition, storagePosition, columnType, columnDefault,
                columnDefaultInfo, table, defaultUUID, autoincStart,
                autoincInc,partitionPosition, useExtrapolation);
        autoinc_create_or_modify_Start_Increment = userChangedWhat;
    }

    /**
     * Constructor for a ColumnDescriptor
     *
     * @param columnName		The name of the column
     * @param columnPosition	The ordinal position of the column
     * @param storagePosition	The ordinal position of the column on disk (Accounts for alter table)
     * @param columnType		A DataTypeDescriptor for the type of
     *				the column
     * @param columnDefault		A DataValueDescriptor representing the
     *							default value of the column, if any
     *							(null if no default)
     * @param columnDefaultInfo		The default info for the column.
     * @param table			A TableDescriptor for the table the
     *						column is in
     * @param defaultUUID			The UUID for the default, if any.
     * @param autoincStart	Start value for an autoincrement column.
     * @param autoincInc	Increment for autoincrement column
     * @param partitionPosition
     * @param useExtrapolation indicates whether we want to do extrapolation for stats estimation
     */

    public ColumnDescriptor(String columnName, int columnPosition, int storagePosition,
                            DataTypeDescriptor columnType, DataValueDescriptor columnDefault,
                            DefaultInfo columnDefaultInfo,
                            TableDescriptor table,
                            UUID defaultUUID, long autoincStart, long autoincInc, int partitionPosition, byte useExtrapolation)
    {
        this.columnName = columnName;
        this.columnPosition = columnPosition;
        this.storagePosition = storagePosition;
        this.columnType = columnType;
        this.columnDefault = columnDefault;
        this.columnDefaultInfo = columnDefaultInfo;
        this.defaultUUID = defaultUUID;
        if (table != null)
        {
            this.table = table;
            this.uuid = table.getUUID();
        }

        assertAutoinc(autoincInc != 0,
                autoincInc,
                columnDefaultInfo);

        this.autoincStart = autoincStart;
        this.autoincValue = autoincStart;
        this.autoincInc = autoincInc;
        this.collectStatistics = allowsStatistics(columnType);
        this.partitionPosition = partitionPosition;
        this.useExtrapolation = useExtrapolation;
    }

    public ColumnDescriptor(String columnName, int columnPosition, int storagePosition,
                            DataTypeDescriptor columnType, DataValueDescriptor columnDefault,
                            DefaultInfo columnDefaultInfo,
                            TableDescriptor table,
                            UUID defaultUUID, long autoincStart, long autoincInc, int partitionPosition) {
         this(columnName,
              columnPosition,
              storagePosition,
              columnType,
              columnDefault,
              columnDefaultInfo,
              table,
              defaultUUID,
              autoincStart,
              autoincInc,
              partitionPosition,
              (byte)0);
    }

    /**
     * Constructor for a ColumnDescriptor.  Used when
     * columnDescriptor doesn't know/care about a table
     * descriptor.
     *
     * @param columnName		The name of the column
     * @param columnPosition	The ordinal position of the column
     * @param storagePosition	The ordinal position of the column on disk (Accounts for alter table)
     * @param columnType		A DataTypeDescriptor for the type of
     *				the column
     * @param columnDefault		A DataValueDescriptor representing the
     *							default value of the column, if any
     *							(null if no default)
     * @param columnDefaultInfo		The default info for the column.
     * @param uuid			A uuid for the object that this column
     *						is in.
     * @param defaultUUID			The UUID for the default, if any.
     * @param autoincStart	Start value for an autoincrement column.
     * @param autoincInc	Increment for autoincrement column
     * @param autoincValue	Current value of the autoincrement column
     */
    public ColumnDescriptor(String columnName, int columnPosition, int storagePosition,
                            DataTypeDescriptor columnType, DataValueDescriptor columnDefault,
                            DefaultInfo columnDefaultInfo,
                            UUID uuid,
                            UUID defaultUUID,
                            long autoincStart, long autoincInc, long autoincValue, int columnSequence){
            this(columnName,
                    columnPosition,
                    storagePosition,
                    columnType,
                    columnDefault,
                    columnDefaultInfo,
                    uuid,
                    defaultUUID,
                    autoincStart,
                    autoincInc,
                    autoincValue,
                    allowsStatistics(columnType),
                    -1, (byte)0);
    }

    public ColumnDescriptor(String columnName,
                            int columnPosition,
                            int storagePosition,
                            DataTypeDescriptor columnType,
                            DataValueDescriptor columnDefault,
                            DefaultInfo columnDefaultInfo,
                            UUID uuid,
                            UUID defaultUUID,
                            long autoincStart,
                            long autoincInc,
                            long autoincValue,
                            boolean collectStats,
                            int partitionPosition,
                            byte useExtrapolation){
        this.columnName = columnName;
        this.columnPosition = columnPosition;
        this.storagePosition = storagePosition;
        this.columnType = columnType;
        this.columnDefault = columnDefault;
        this.columnDefaultInfo = columnDefaultInfo;
        this.uuid = uuid;
        this.defaultUUID = defaultUUID;
        this.collectStatistics = collectStats;
        assertAutoinc(autoincInc!=0, autoincInc, columnDefaultInfo);
        this.autoincStart = autoincStart;
        this.autoincValue = autoincValue;
        this.autoincInc = autoincInc;
        this.partitionPosition = partitionPosition;
        this.useExtrapolation = useExtrapolation;
    }

    public boolean collectStatistics(){
        return collectStatistics;
    }

    public void setCollectStatistics(boolean collectStatistics){
        this.collectStatistics = collectStatistics;
    }

    public byte getUseExtrapolation() {
        return useExtrapolation;
    }

    public void setUseExtrapolation(byte useExtrapolation) {
        this.useExtrapolation = useExtrapolation;
    }

    /**
     * Get the UUID of the object the column is a part of.
     *
     * @return	The UUID of the table the column is a part of.
     */
    public UUID	getReferencingUUID()
    {
        return uuid;
    }

    /**
     * Get the TableDescriptor of the column's table.
     *
     * @return	The TableDescriptor of the column's table.
     */
    public TableDescriptor	getTableDescriptor()
    {
        return table;
    }

    /**
     * Get the name of the column.
     *
     * @return	A String containing the name of the column.
     */
    public String	getColumnName()
    {
        return columnName;
    }

    /**
     * Sets the column name in case of rename column.
     *
     * @param newColumnName	The new column name.
     */
    public void	setColumnName(String newColumnName)
    {
        this.columnName = newColumnName;
    }

    /**
     * Sets the table descriptor for the column.
     *
     * @param tableDescriptor	The table descriptor for this column
     */
    public void	setTableDescriptor(TableDescriptor tableDescriptor)
    {
        this.table = tableDescriptor;
    }

    /**
     *
     * For example, if you had a 3 column table where you add and drop a column 1K times and then add it again, that columns position will
     * still be 4.
     *
     * Get the ordinal position of the column (1 based).  This is the position of the column on storage.
     *
     *
     *
     * @return	The ordinal position of the column.
     */
    public int	getPosition()
    {
        return columnPosition;
    }

    /**
     * For example, if you had a 3 column table where you add and drop a column 1K times and then add it again, that columns position will
     * still be 1,004.
     *
     * @return	The ordinal position of the column.
     */
    public int	getStoragePosition() {
        return storagePosition;
    }


    /**
     * Get the TypeDescriptor of the column's datatype.
     *
     * @return	The TypeDescriptor of the column's datatype.
     */
    public DataTypeDescriptor getType()
    {
        return columnType;
    }

    /**
     * Return whether or not there is a non-null default on this column.
     *
     * @return Whether or not there is a non-null default on this column.
     */
    public boolean hasNonNullDefault() {
        return columnDefault != null && !columnDefault.isNull() || columnDefaultInfo != null;

    }

    /**
     * Get the default value for the column. For columns with primitive
     * types, the object returned will be of the corresponding object type.
     * For example, for a float column, getDefaultValue() will return
     * a Float.
     *
     * @return	An object with the value and type of the default value
     *		for the column. Returns NULL if there is no default.
     */
    public DataValueDescriptor getDefaultValue()
    {
        return columnDefault;
    }

    /**
     * Get the DefaultInfo for this ColumnDescriptor.
     *
     * @return The DefaultInfo for this ColumnDescriptor.
     */
    public DefaultInfo getDefaultInfo()
    {
        return columnDefaultInfo;
    }

    /**
     * Get the UUID for the column default, if any.
     *
     * @return The UUID for the column default, if any.
     */
    public UUID getDefaultUUID()
    {
        return defaultUUID;
    }

    /**
     * Get a DefaultDescriptor for the default, if any, associated with this column.
     *
     * @param	dd	The DataDictionary.
     *
     * @return	A DefaultDescriptor if this column has a column default.
     */
    public DefaultDescriptor getDefaultDescriptor(DataDictionary dd)
    {
        DefaultDescriptor defaultDescriptor = null;

        if (defaultUUID != null)
        {
            defaultDescriptor = new DefaultDescriptor(dd, defaultUUID, uuid, columnPosition);
        }

        return defaultDescriptor;
    }

    /**
     * Is this column an autoincrement column?
     *
     * @return Whether or not this is an autoincrement column
     */
    public boolean isAutoincrement()
    {
        return (autoincInc != 0);
    }
    public boolean updatableByCursor()
    {
        return false;
    }

    /**
     * Is this column a generated column
     */
    public boolean hasGenerationClause() {
        return columnDefaultInfo != null && columnDefaultInfo.isGeneratedColumn();
    }

    /**
     * Is this column to have autoincremented value always ?
     */
    public boolean isAutoincAlways(){
        return (columnDefaultInfo == null) && isAutoincrement();
    }

    /**
     *
     * Position in external table partition logic.
     *
     * @return
     */
    public int getPartitionPosition() {
        return partitionPosition;
    }

    /**
     * Get the start value of an autoincrement column
     *
     * @return Get the start value of an autoincrement column
     */
    public long getAutoincStart()
    {
        return autoincStart;
    }

    /**
     * Get the Increment value given by the user for an autoincrement column
     *
     * @return the Increment value for an autoincrement column
     */
    public long getAutoincInc()
    {
        return autoincInc;
    }

    /**
     * Get the current value for an autoincrement column.
     *
     * One case in which this is used involves dropping a column
     * from a table. When ALTER TABLE DROP COLUMN runs, it drops
     * the column from SYSCOLUMNS, and then must adjust the
     * column positions of the other subsequent columns in the table
     * to account for the removal of the dropped columns. This
     * involves deleting and re-adding the column descriptors to
     * SYSCOLUMNS, but during that process we must be careful to
     * preserve the current value of any autoincrement column.
     *
     * @return the current value for an autoincrement column
     */
    public long getAutoincValue()
    {
        return autoincValue;
    }

    public long getAutoinc_create_or_modify_Start_Increment()
    {
        return autoinc_create_or_modify_Start_Increment;
    }
    public void setAutoinc_create_or_modify_Start_Increment(int c_or_m)
    {
        autoinc_create_or_modify_Start_Increment = c_or_m;
    }

    /**
     * Set the ordinal position of the column.
     */
    public void	setPosition(int columnPosition)
    {
        this.columnPosition = columnPosition;
    }

    /**
     * Convert the ColumnDescriptor to a String.
     *
     * @return	A String representation of this ColumnDescriptor
     */

    public String	toString()
    {
        if (SanityManager.DEBUG)
        {
			/*
			** NOTE: This does not format table, because table.toString()
			** formats columns, leading to infinite recursion.
			*/
            return "columnName: " + columnName + "\n" +
                    "columnPosition: " + columnPosition + "\n" +
                    "storagePosition: " + storagePosition + "\n" +
                    "columnType: " + columnType + "\n" +
                    "columnDefault: " + columnDefault + "\n" +
                    "uuid: " + uuid + "\n" +
                    "defaultUUID: " + defaultUUID + "\n" +
                    "useExtrapolation: " + useExtrapolation;
        }
        else
        {
            return "";
        }
    }

    public boolean isEquivalent(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDescriptor that = (ColumnDescriptor) o;

        if (table != null ? !table.equals(that.table) : that.table != null) return false;
        return (columnName != null ? columnName.equals(that.columnName) : that.columnName == null) && !(columnType != null ? !columnType.equals(that.columnType) : that.columnType != null);

    }

    /** @see TupleDescriptor#getDescriptorName */
    public String getDescriptorName()
    {
        // try and get rid of getColumnName!
        return columnName;
    }

    /** @see TupleDescriptor#getDescriptorType */
    public String getDescriptorType()
    {
        return "Column";
    }


    private static void assertAutoinc(boolean autoinc,
                                      long autoincInc,
                                      DefaultInfo defaultInfo){

        if (SanityManager.DEBUG) {
            if (autoinc){
                SanityManager.ASSERT((autoincInc != 0),
                        "increment is zero for  autoincrement column");
                SanityManager.ASSERT((defaultInfo == null ||
                                defaultInfo.isDefaultValueAutoinc()),
                        "If column is autoinc and have defaultInfo, " +
                                "isDefaultValueAutoinc must be true.");
            }
            else{
                SanityManager.ASSERT((autoincInc == 0),
                        "increment is non-zero for non-autoincrement column");
                SanityManager.ASSERT((defaultInfo == null ||
                                ! defaultInfo.isDefaultValueAutoinc()),
                        "If column is not autoinc and have defaultInfo, " +
                                "isDefaultValueAutoinc can not be true");
            }
        }
    }


    public static boolean allowsStatistics(int typeFormatId){
        switch(typeFormatId){
            case StoredFormatIds.SQL_BOOLEAN_ID:
            case StoredFormatIds.SQL_TINYINT_ID:
            case StoredFormatIds.SQL_SMALLINT_ID:
            case StoredFormatIds.SQL_INTEGER_ID:
            case StoredFormatIds.SQL_LONGINT_ID:
            case StoredFormatIds.SQL_REAL_ID:
            case StoredFormatIds.SQL_DOUBLE_ID:
            case StoredFormatIds.SQL_DECIMAL_ID:
            case StoredFormatIds.SQL_CHAR_ID:
            case StoredFormatIds.SQL_DATE_ID:
            case StoredFormatIds.SQL_TIME_ID:
            case StoredFormatIds.SQL_TIMESTAMP_ID:
            case StoredFormatIds.SQL_VARCHAR_ID:
            case StoredFormatIds.SQL_LONGVARCHAR_ID:
            case StoredFormatIds.SQL_ARRAY_ID:
                return true;
            default:
                return false;
        }
    }

    public static boolean allowsStatistics(DataTypeDescriptor columnType) {
        try {
            return allowsStatistics(columnType.getNull().getTypeFormatId());
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    public ConglomerateDescriptor getBaseConglomerateDescriptor() {
        return getTableDescriptor()==null?null:getTableDescriptor().getBaseConglomerateDescriptor();
    }

    public static boolean allowsExtrapolation(int typeFormatId){
        switch(typeFormatId){
            case StoredFormatIds.SQL_TINYINT_ID:
            case StoredFormatIds.SQL_SMALLINT_ID:
            case StoredFormatIds.SQL_INTEGER_ID:
            case StoredFormatIds.SQL_LONGINT_ID:
            case StoredFormatIds.SQL_REAL_ID:
            case StoredFormatIds.SQL_DOUBLE_ID:
            case StoredFormatIds.SQL_DECIMAL_ID:
            case StoredFormatIds.SQL_DATE_ID:
            case StoredFormatIds.SQL_TIMESTAMP_ID:
                return true;
            default:
                return false;
        }
    }

    public static boolean allowsExtrapolation(DataTypeDescriptor columnType) {
        try {
            return allowsExtrapolation(columnType.getNull().getTypeFormatId());
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }

    }

}
