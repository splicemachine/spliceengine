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

import java.util.TreeMap;

import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import splice.com.google.common.primitives.Ints;

/**
 * This class represents a table descriptor. The external interface to this
 * class is:
 * <p/>
 * <ol>
 * <li>external interface </li>
 * <li>public String	getSchemaName();</li>
 * <li>public String	getQualifiedName();</li>
 * <li>public int	getTableType();</li>
 * <li>public long getHeapConglomerateId() throws StandardException;</li>
 * <li>public int getNumberOfColumns();		</li>
 * <li>public FormatableBitSet getReferencedColumnMap();</li>
 * <li>public void setReferencedColumnMap(FormatableBitSet referencedColumnMap);</li>
 * <li>public int getMaxColumnID() throws StandardException;</li>
 * <li>public void	setUUID(UUID uuid);</li>
 * <li>public char	getLockGranularity();</li>
 * <li>public void	setTableName(String newTableName);</li>
 * <li>public void	setLockGranularity(char lockGranularity);</li>
 * <li>public ExecRow getEmptyExecRow( ContextManager cm) throws StandardException;</li>
 * <li>public boolean tableNameEquals(String otherSchemaName, String otherTableName);</li>
 * <li>public ReferencedKeyConstraintDescriptor getPrimaryKey() throws StandardException;</li>
 * <li>public void removeConglomerateDescriptor(ConglomerateDescriptor cd)	throws StandardException;</li>
 * <li>public void removeConstraintDescriptor(ConstraintDescriptor cd)	throws StandardException;</li>
 * <li>public void getAffectedIndexes(...) throws StandardException;</li>
 * <li>public void	getAllRelevantTriggers(...) throws StandardException;</li>
 * <li>public void getAllRelevantConstraints(...) throws StandardException</li>
 * <li>public ColumnDescriptorList getColumnDescriptorList();</li>
 * <li> public String[] getColumnNamesArray();</li>
 * <li>public long[]   getAutoincIncrementArray();</li>
 * <li>public ColumnDescriptor	getColumnDescriptor(String columnName);</li>
 * <li>public ColumnDescriptor	getColumnDescriptor(int columnNumber);</li>
 * <li>public ConglomerateDescriptor[]	getConglomerateDescriptors() throws StandardException;</li>
 * <li>public ConglomerateDescriptor	getConglomerateDescriptor(long conglomerateNumber)	throws StandardException;</li>
 * <li>public ConglomerateDescriptor	getConglomerateDescriptor(UUID conglomerateUUID) throws StandardException;</li>
 * <li>public	IndexLister	getIndexLister() throws StandardException;</li>
 * <li>public ViewDescriptor getViewDescriptor();</li>
 * <li>public boolean tableHasAutoincrement();</li>
 * <li>public boolean statisticsExist(ConglomerateDescriptor cd) throws StandardException;</li>
 * <li>public double selectivityForConglomerate(...)throws StandardException;</li>
 * </ol>
 * <p/>
 */

public class TableDescriptor extends TupleDescriptor implements UniqueSQLObjectDescriptor, Provider, Dependent{
    public static final int BASE_TABLE_TYPE=0;
    public static final int SYSTEM_TABLE_TYPE=1;
    public static final int VIEW_TYPE=2;
    public static final int GLOBAL_TEMPORARY_TABLE_TYPE=3;
    public static final int SYNONYM_TYPE=4;
    public static final int VTI_TYPE=5;
    /* Supports with clauses for TPCDS*/
    public static final int WITH_TYPE=6;


    public static final int EXTERNAL_TYPE=7;
    public static final int[] EMPTY_PARTITON_ARRAY = new int[0];
    public static final char ROW_LOCK_GRANULARITY='R';
    public static final char TABLE_LOCK_GRANULARITY='T';
    public static final char DEFAULT_LOCK_GRANULARITY=ROW_LOCK_GRANULARITY;

    // Constants for the automatic index statistics update feature.
    public static final int ISTATS_CREATE_THRESHOLD;
    public static final int ISTATS_ABSDIFF_THRESHOLD;
    public static final double ISTATS_LNDIFF_THRESHOLD;

    static{
        ISTATS_CREATE_THRESHOLD=PropertyUtil.getSystemInt(
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_CREATE_THRESHOLD,
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_CREATE_THRESHOLD_DEFAULT
        );
        ISTATS_ABSDIFF_THRESHOLD=PropertyUtil.getSystemInt(
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_ABSDIFF_THRESHOLD,
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_ABSDIFF_THRESHOLD_DEFAULT);
        double tmpLog2Diff=
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_LNDIFF_THRESHOLD_DEFAULT;
        try{
            String tmpStr=PropertyUtil.getSystemProperty(
                    Property.STORAGE_AUTO_INDEX_STATS_DEBUG_LNDIFF_THRESHOLD);
            if(tmpStr!=null){
                tmpLog2Diff=Double.parseDouble(tmpStr);
            }
        }catch(NumberFormatException nfe){
            // Ignore, use the default.
        }
        ISTATS_LNDIFF_THRESHOLD=tmpLog2Diff;
    }

    /**
     */

    // implementation
    private char lockGranularity;
    private boolean onCommitDeleteRows; //true means on commit delete rows, false means on commit preserve rows of temporary table.
    private boolean onRollbackDeleteRows; //true means on rollback delete rows. This is the only value supported.
    String tableName;
    UUID oid;
    int tableType;
    String tableVersion;
    private int columnSequence;
    private String delimited;
    private String escaped;
    private String lines;
    private String storedAs;
    private String location;
    private String compression;
    // NOT USED ANYMORE, for backward compatibility only
    @Deprecated
    private boolean isPinned;
    private boolean purgeDeletedRows;
    private Long minRetentionPeriod;

    /**
     * <p>
     * The id of the heap conglomerate for the table described by this
     * instance. The value -1 means it's uninitialized, in which case it
     * will be initialized lazily when {@link #getHeapConglomerateId()} is
     * called.
     * </p>
     * <p/>
     * <p>
     * It is declared volatile to ensure that concurrent callers of
     * {@code getHeapConglomerateId()} while {@code heapConglomNumber} is
     * uninitialized, will either see the value -1 or the fully initialized
     * conglomerate number, and never see a partially initialized value
     * (as was the case in DERBY-5358 because reads/writes of a long field are
     * not guaranteed to be atomic unless the field is declared volatile).
     * </p>
     */
    private volatile long heapConglomNumber=-1;

    ColumnDescriptorList columnDescriptorList;
    ConglomerateDescriptorList conglomerateDescriptorList;
    ConstraintDescriptorList constraintDescriptorList;
    private GenericDescriptorList triggerDescriptorList;
    ViewDescriptor viewDescriptor;
    private SchemaDescriptor schemaDesctiptor;

    private FormatableBitSet referencedColumnMapGet(){

        LanguageConnectionContext lcc=
                (LanguageConnectionContext)ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);

        assert lcc!=null;

        return lcc.getReferencedColumnMap(this);

    }

    private void referencedColumnMapPut(FormatableBitSet newReferencedColumnMap){

        LanguageConnectionContext lcc=
                (LanguageConnectionContext)ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);

        assert lcc!=null || newReferencedColumnMap==null;

        // This method is called with a null argument at database
        // creation time when there is no lcc, cf stack trace in the
        // JIRA for DERBY-4895, we can safely ignore that, as there
        // exists no referencedColumnMap yet.
        if(lcc!=null){
            lcc.setReferencedColumnMap(this,newReferencedColumnMap);
        }
    }

    /**
     * Constructor for a TableDescriptor (this is for a temporary table).
     *
     * @param dataDictionary       The data dictionary that this descriptor lives in
     * @param tableName            The name of the temporary table
     * @param schema               The schema descriptor for this table.
     * @param tableType            An integer identifier for the type of the table : declared global temporary table
     * @param onCommitDeleteRows   If true, on commit delete rows else on commit preserve rows of temporary table.
     * @param onRollbackDeleteRows If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
     */
    public TableDescriptor(DataDictionary dataDictionary,
                           String tableName,
                           SchemaDescriptor schema,
                           int tableType,
                           boolean onCommitDeleteRows,
                           boolean onRollbackDeleteRows, int numberOfColumns){
        this(dataDictionary,tableName,schema,tableType,'\0',numberOfColumns,null,null,null,null,null,null,false,false, null);
        this.onCommitDeleteRows=onCommitDeleteRows;
        this.onRollbackDeleteRows=onRollbackDeleteRows;
    }

    /**
     * Constructor for a TableDescriptor.
     *
     * @param dataDictionary  The data dictionary that this descriptor lives in
     * @param tableName       The name of the table
     * @param schema          The schema descriptor for this table.
     * @param tableType       An integer identifier for the type of the table (base table, view, etc.)
     * @param lockGranularity The lock granularity.
     */
    public TableDescriptor(DataDictionary dataDictionary,
                           String tableName,
                           SchemaDescriptor schema,
                           int tableType,
                           char lockGranularity, int numberOfColumns,
                           String delimited,
                           String escaped,
                           String lines,
                           String storedAs,
                           String location,
                           String compression,
                           boolean isPinned,
                           boolean purgeDeletedRows,
                           Long minRetentionPeriod
    ){
        super(dataDictionary);

        this.schemaDesctiptor=schema;
        this.tableName=tableName;
        this.tableType=tableType;
        this.lockGranularity=lockGranularity;
        this.columnSequence = numberOfColumns;
        this.conglomerateDescriptorList=new ConglomerateDescriptorList();
        this.columnDescriptorList=new ColumnDescriptorList();
        this.constraintDescriptorList=new ConstraintDescriptorList();
        this.triggerDescriptorList=new GenericDescriptorList();
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;
        this.compression = compression;
        // NOT USED ANYMORE, for backward compatibility only
        this.isPinned = isPinned;
        this.purgeDeletedRows = purgeDeletedRows;
        this.minRetentionPeriod = minRetentionPeriod;
    }

    //
    // TableDescriptor interface
    //

    /**
     * Gets the name of the schema the table lives in.
     *
     * @return A String containing the name of the schema the table
     * lives in.
     */
    public String getSchemaName(){
        return schemaDesctiptor.getSchemaName();
    }

    public int getColumnSequence() {
        return columnSequence;
    }

    public void setColumnSequence(int columnSequence) {
        this.columnSequence = columnSequence;
    }


    /**
     *
     * Compression Type,
     *
     * @return
     */
    public String getCompression() {
        return compression;
    }

    /**
     *
     * Location for an external table
     *
     * @return
     */
    public String getLocation() {
        return location;
    }

    /**
     *
     * Storage Format
     *
     * @return
     */
    public String getStoredAs() {
        return storedAs;
    }

    /**
     *
     * Lines Terminator
     *
     * @return
     */
    public String getLines() {
        return lines;
    }

    /**
     *
     * Escape string
     *
     * @return
     */
    public String getEscaped() {
        return escaped;
    }

    /**
     *
     * Delimitter string
     *
     * @return
     */
    public String getDelimited() {
        return delimited;
    }

    /**
     * Will tell if the current table is currently pinned in the memory
     * @return
     */
    @Deprecated
    public boolean isPinned() {
        return isPinned;
    }

    /**
     * Will mark the table pinned
     * @param pinned
     */
    @Deprecated
    public void setPinned(boolean pinned) {
        isPinned = pinned;
    }

    public boolean purgeDeletedRows() {
        return purgeDeletedRows;
    }

    public void setPurgeDeletedRows(boolean purgeDeletedRows) {
        this.purgeDeletedRows = purgeDeletedRows;
    }

    public Long getMinRetainedVersions() {
        return minRetentionPeriod;
    }

    public void setMinRetainedVersions(Long minRetentionPeriod) {
        this.minRetentionPeriod = minRetentionPeriod;
    }

    /**
     * Gets the SchemaDescriptor for this TableDescriptor.
     *
     * @return SchemaDescriptor    The SchemaDescriptor.
     */
    @Override
    public SchemaDescriptor getSchemaDescriptor(){
        return schemaDesctiptor;
    }

    /**
     * Gets the name of the table.
     *
     * @return A String containing the name of the table.
     */
    @Override
    public String getName(){
        return tableName;
    }

    /**
     * Sets the the table name in case of rename table.
     * <p/>
     * This is used only by rename table
     *
     * @param newTableName The new table name.
     */
    public void setTableName(String newTableName){
        this.tableName=newTableName;
    }

    /**
     * Gets the full, qualified name of the table.
     *
     * @return A String containing the name of the table.
     */
    public String getQualifiedName(){
        return IdUtil.mkQualifiedName(getSchemaName(),getName());
    }

    /**
     * Gets the UUID of the table.
     *
     * @return The UUID of the table.
     */
    @Override
    public UUID getUUID(){
        return oid;
    }

    /**
     * Gets an identifier telling what type of table this is
     * (base table, declared global temporary table, view, etc.)
     *
     * @return An identifier telling what type of table this is.
     */
    public int getTableType(){
        return tableType;
    }

    /**
     * Gets the id for the heap conglomerate of the table.
     * There may also be keyed conglomerates, these are
     * stored separately in the conglomerates table.
     *
     * @return the id of the heap conglomerate for the table.
     * @throws StandardException Thrown on error
     */
    public long getHeapConglomerateId() throws StandardException{

		// If we've already cached the heap conglomerate number, then
        // simply return it.
        //

        if (tableType != BASE_TABLE_TYPE && tableType != SYSTEM_TABLE_TYPE &&
                tableType != EXTERNAL_TYPE && tableType != GLOBAL_TEMPORARY_TABLE_TYPE) {
            return -1;
        }

		if (heapConglomNumber != -1) {
			return heapConglomNumber;
		}

        ConglomerateDescriptor[] cds=getConglomerateDescriptors();

        ConglomerateDescriptor cd=null;
        for(ConglomerateDescriptor cdCheck : cds){
            cd=cdCheck;
            if(!cd.isIndex())
                break;
        }

        assert cd!=null:"cd is expected to be non-null for "+tableName;
        assert !cd.isIndex():"Did not find heap conglomerate for "+tableName;

        heapConglomNumber=cd.getConglomerateNumber();

        return heapConglomNumber;
    }

    /**
     * Gets the number of columns in the table.
     *
     * @return the number of columns in the table.
     */
    public int getNumberOfColumns(){
        return getColumnDescriptorList().size();
    }

    /**
     * Get the referenced column map of the table.
     *
     * @return the referencedColumnMap of the table.
     */
    public FormatableBitSet getReferencedColumnMap(){
        return referencedColumnMapGet();
    }

    /**
     * Set the referenced column map of the table.
     *
     * @param referencedColumnMap FormatableBitSet of referenced columns.
     */
    public void setReferencedColumnMap(FormatableBitSet referencedColumnMap){
        referencedColumnMapPut(referencedColumnMap);
    }

    /**
     * Given a list of columns in the table, construct a bit  map of those
     * columns' ids.
     *
     * @param cdl list of columns whose positions we want to record in the bit map
     */
    public FormatableBitSet makeColumnMap(ColumnDescriptorList cdl){
        FormatableBitSet result=new FormatableBitSet(columnDescriptorList.size()+1);
        int count=cdl.size();

        for(int i=0;i<count;i++){
            ColumnDescriptor cd=cdl.elementAt(i);

            result.set(cd.getPosition());
        }

        return result;
    }

    /**
     * Gets the highest column position id in the table.
     *
     * @return the highest column id in the table
     * @see {@link #getMaxStorageColumnID()}
     */
    public int getMaxColumnID() {
        int maxColumnID=1;
        int cdlSize=getColumnDescriptorList().size();
        for(int index=0;index<cdlSize;index++){
            ColumnDescriptor cd=columnDescriptorList.elementAt(index);
            maxColumnID=Math.max(maxColumnID,cd.getPosition());
        }
        return maxColumnID;
    }

    /**
     * Gets the highest column storage position in the table.
     *
     * @return the highest column storage position in the table
     * @see {@link #getMaxColumnID()}
     */
    public int getMaxStorageColumnID() {
        int maxColumnID=1;
        int cdlSize=getColumnDescriptorList().size();
        for(int index=0;index<cdlSize;index++){
            ColumnDescriptor cd=columnDescriptorList.elementAt(index);
            maxColumnID=Math.max(maxColumnID,cd.getStoragePosition());
        }
        return maxColumnID;
    }

    public int[] getStoragePositionArray() {
        int[] storagePositionArray = new int[getColumnDescriptorList().size()];
        for (int i = 0; i < getColumnDescriptorList().size(); i++) {
            storagePositionArray[i] = getColumnDescriptor(i+1).getStoragePosition()-1;
        }
        return storagePositionArray;
    }

    /**
     * Sets the UUID of the table
     *
     * @param oid The UUID of the table to be set in the descriptor
     */
    public void setUUID(UUID oid){
        this.oid=oid;
    }

    public void setVersion(String version){
        this.tableVersion=version;
    }

    public String getVersion(){
        return this.tableVersion;
    }

    /**
     * Gets the lock granularity for the table.
     *
     * @return A char representing the lock granularity for the table.
     */
    public char getLockGranularity(){
        return lockGranularity;
    }

    /**
     * Sets the lock granularity for the table to the specified value.
     *
     * @param lockGranularity The new lockGranularity.
     */
    public void setLockGranularity(char lockGranularity){
        this.lockGranularity=lockGranularity;
    }

    /**
     * Gets the on rollback behavior for the declared global temporary table.
     *
     * @return A boolean representing the on rollback behavior for the declared global temporary table.
     */
    public boolean isOnRollbackDeleteRows(){
        return onRollbackDeleteRows;
    }

    /**
     * Gets the on commit behavior for the declared global temporary table.
     *
     * @return A boolean representing the on commit behavior for the declared global temporary table.
     */
    public boolean isOnCommitDeleteRows(){
        return onCommitDeleteRows;
    }

    /**
     * Sets the heapConglomNumber to -1 for temporary table since the table was dropped and recreated at the commit time
     * and hence its conglomerate id has changed. This is used for temporary table descriptors only
     */
    public void resetHeapConglomNumber(){
        assert tableType==TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE:"tableType expected to be GLOBAL_TEMPORARY_TABLE_TYPE";
        heapConglomNumber=-1;
    }

    public void setHeapConglomNumber(long heapConglomNumber){
        this.heapConglomNumber = heapConglomNumber;
    }
    /**
     * Gets an ExecRow for rows stored in the table this describes.
     *
     * @return the row.
     * @throws StandardException Thrown on failure
     */
    public ExecRow getEmptyExecRow() throws StandardException{
        int columnCount=getNumberOfColumns();
        ExecRow result=getDataDictionary().getExecutionFactory().getValueRow(columnCount);

        for(int index=0;index<columnCount;index++){
            ColumnDescriptor cd=columnDescriptorList.elementAt(index);
            //String name = column.getColumnName();
            DataValueDescriptor dataValue=cd.getType().getNull();
            result.setColumn(index+1,dataValue);
        }
        return result;
    }

    /**
     * Return an array of collation ids for this table.
     * <p/>
     * Return an array of collation ids, one for each column in the
     * columnDescriptorList.  This is useful for passing collation id info
     * down to store, for instance in createConglomerate().
     * <p/>
     * This is only expected to get called during ddl, so object allocation
     * is ok.
     *
     * @throws StandardException Standard exception policy.
     */
    public int[] getColumnCollationIds() throws StandardException{
        int[] collation_ids=new int[getNumberOfColumns()];

        for(int index=0;index<collation_ids.length;index++){
            ColumnDescriptor cd=columnDescriptorList.elementAt(index);

            collation_ids[index]=cd.getType().getCollationType();

        }
        return (collation_ids);

    }

    /**
     * Gets the conglomerate descriptor list
     *
     * @return The conglomerate descriptor list for this table descriptor
     */
    public ConglomerateDescriptorList getConglomerateDescriptorList(){
        return conglomerateDescriptorList;
    }

    /**
     * Gets the view descriptor for this TableDescriptor.
     *
     * @return ViewDescriptor    The ViewDescriptor, if any.
     */
    public ViewDescriptor getViewDescriptor(){
        return viewDescriptor;
    }

    /**
     * Set (cache) the view descriptor for this TableDescriptor
     *
     * @param viewDescriptor The view descriptor to cache.
     */
    public void setViewDescriptor(ViewDescriptor viewDescriptor){
//        assert tableType==TableDescriptor.VIEW_TYPE:"tableType expected to be TableDescriptor.VIEW_TYPE";
        this.viewDescriptor=viewDescriptor;
    }

    /**
     * Is this provider persistent?  A stored dependency will be required
     * if both the dependent and provider are persistent.
     *
     * @return boolean              Whether or not this provider is persistent.
     */
    @Override
    public boolean isPersistent(){
        return tableType!=TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE && (super.isPersistent());
    }

    /**
     * Is this descriptor represents a synonym?
     *
     * @return boolean              Whether or not this represents a synonym
     */
    public boolean isSynonymDescriptor(){
        return tableType==TableDescriptor.SYNONYM_TYPE;
    }

    /**
     * Gets the number of indexes on the table, including the backing indexes.
     *
     * @return the number of columns in the table.
     * @see #getQualifiedNumberOfIndexes
     */
    public int getTotalNumberOfIndexes() throws StandardException{
        return getQualifiedNumberOfIndexes(0,false);
    }

    /**
     * Returns the number of indexes matching the criteria.
     *
     * @param minColCount            the minimum number of ordered columns in the indexes
     *                               we want to count
     * @param nonUniqeTrumpsColCount if {@code true} a non-unique index will be
     *                               included in the count even if it has less than {@code minColCount}
     *                               ordered columns
     * @return Number of matching indexes.
     * @see #getTotalNumberOfIndexes()
     */
    public int getQualifiedNumberOfIndexes(int minColCount,boolean nonUniqeTrumpsColCount){
        int matches=0;
        for(ConglomerateDescriptor cd : conglomerateDescriptorList){
            if(cd.isIndex()){
                IndexRowGenerator irg=cd.getIndexDescriptor();
                if(irg.numberOfOrderedColumns()>=minColCount || (nonUniqeTrumpsColCount && !irg.isUnique())){
                    matches++;
                }
            }
        }
        return matches;
    }

    /**
     * Builds a list of all triggers which are relevant to a
     * given statement type, given a list of updated columns.
     *
     * @param statementType    defined in StatementType
     * @param changedColumnIds array of changed columns
     * @param relevantTriggers IN/OUT. Passed in as an empty list. Filled in as we go.
     * @throws StandardException Thrown on error
     */
    public void getAllRelevantTriggers(int statementType,int[] changedColumnIds,GenericDescriptorList relevantTriggers) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.ASSERT((statementType==StatementType.INSERT) ||
                            (statementType==StatementType.BULK_INSERT_REPLACE) ||
                            (statementType==StatementType.UPDATE) ||
                            (statementType==StatementType.DELETE),
                    "invalid statement type "+statementType);
        }

        DataDictionary dd=getDataDictionary();
        for(Object o : dd.getTriggerDescriptors(this)){
            TriggerDescriptor tgr=(TriggerDescriptor)o;
            if(tgr.needsToFire(statementType,changedColumnIds)){
                relevantTriggers.add(tgr);
            }
        }
    }

    /**
     * Gets all of the relevant constraints for a statement, given its
     * statement type and its list of updated columns.
     *
     * @param statementType           As defined in StatementType.
     * @param skipCheckConstraints    Skip check constraints
     * @param changedColumnIds        If null, all columns being changed, otherwise array
     *                                of 1-based column ids for columns being changed
     * @param needsDeferredProcessing IN/OUT. true if the statement already needs
     *                                deferred processing. set while evaluating this
     *                                routine if a trigger or constraint requires
     *                                deferred processing
     * @param relevantConstraints     IN/OUT. Empty list is passed in. We hang constraints on it as we go.
     * @throws StandardException Thrown on error
     */
    public void getAllRelevantConstraints(int statementType,
                                          boolean skipCheckConstraints,
                                          int[] changedColumnIds,
                                          boolean[] needsDeferredProcessing,
                                          ConstraintDescriptorList relevantConstraints) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.ASSERT((statementType==StatementType.INSERT) ||
                            (statementType==StatementType.BULK_INSERT_REPLACE) ||
                            (statementType==StatementType.UPDATE) ||
                            (statementType==StatementType.DELETE),
                    "invalid statement type "+statementType);
        }

        DataDictionary dd=getDataDictionary();
        ConstraintDescriptorList cdl=dd.getConstraintDescriptors(this);
        int cdlSize=cdl.size();

        for(int index=0;index<cdlSize;index++){
            ConstraintDescriptor cd=cdl.elementAt(index);

            if(skipCheckConstraints &&
                    (cd.getConstraintType()==DataDictionary.CHECK_CONSTRAINT)){
                continue;
            }

			/*
            ** For each constraint, figure out if it requires deferred processing.
			** Note that we need to do this on constraints that don't
			** necessarily need to fire -- e.g. for an insert into a
			** a table with a self-referencing constraint, we don't
			** need to check the primary key constraint (assuming it
			** is only referenced by the self-referencing fk on the same
			** table), but we have to run in deferred mode nonetheless
			** (even though we aren't going to check the pk constraint).
			*/
            if(!needsDeferredProcessing[0] &&
                    (cd instanceof ReferencedKeyConstraintDescriptor) &&
                    (statementType!=StatementType.UPDATE &&
                            statementType!=StatementType.BULK_INSERT_REPLACE)){
                /* For insert (bulk or regular) on a non-published table,
                 * we only need deferred mode if there is a
				 * self-referencing foreign key constraint.
				 */
                needsDeferredProcessing[0]=((ReferencedKeyConstraintDescriptor)cd).
                        hasSelfReferencingFK(cdl,ConstraintDescriptor.ENABLED);
            }

            if(cd.needsToFire(statementType,changedColumnIds)){
                /*
                ** For update, if we are updating a referenced key, then
				** we have to do it in deferred mode (in case we update
				** multiple rows).
				*/
                if((cd instanceof ReferencedKeyConstraintDescriptor) &&
                        (statementType==StatementType.UPDATE ||
                                statementType==StatementType.BULK_INSERT_REPLACE)){
                    needsDeferredProcessing[0]=true;
                }

                relevantConstraints.add(cd);
            }
        }
    }

    @Override
    public DependableFinder getDependableFinder(){
        if(referencedColumnMapGet()==null)
            return getDependableFinder(StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID);
        else
            return getColumnDependableFinder
                    (StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID,
                            referencedColumnMapGet().getByteArray());
    }

    @Override
    public String getObjectName(){
        if(referencedColumnMapGet()==null)
            return tableName;
        else{
            StringBuilder name=new StringBuilder();
            name.append(tableName);
            boolean first=true;

            for(int i=0;i<columnDescriptorList.size();i++){
                ColumnDescriptor cd=columnDescriptorList.elementAt(i);
                if(referencedColumnMapGet().isSet(cd.getPosition())){
                    if(first){
                        name.append("(").append(cd.getColumnName());
                        first=false;
                    }else
                        name.append(", ").append(cd.getColumnName());
                }
            }
            if(!first)
                name.append(")");
            return name.toString();
        }
    }

    /**
     * Get the provider's UUID
     *
     * @return String    The provider's UUID
     */
    @Override
    public UUID getObjectID(){
        return oid;
    }

    /**
     * Get the provider's type.
     *
     * @return String        The provider's type.
     */
    @Override
    public String getClassType(){
        return Dependable.TABLE;
    }

    /**
     * Prints the contents of the TableDescriptor
     *
     * @return The contents as a String
     */
    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            String tempString=
                    "\n"+"schema: "+schemaDesctiptor+"\n"+
                            "tableName: "+tableName+"\n"+
                            "oid: "+oid+" tableType: "+tableType+"\n"+
                            "conglomerateDescriptorList: "+conglomerateDescriptorList+"\n"+
                            "columnDescriptorList: "+columnDescriptorList+"\n"+
                            "constraintDescriptorList: "+constraintDescriptorList+"\n"+
                            "heapConglomNumber: "+heapConglomNumber+"\n";
            if(tableType==TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE){
                tempString=tempString+"onCommitDeleteRows: "+"\n"+
                        onCommitDeleteRows+"\n";
                tempString=tempString+"onRollbackDeleteRows: "+"\n"+
                        onRollbackDeleteRows;
            }else
                tempString=tempString+"lockGranularity: "+lockGranularity;
            return tempString;
        }else{
            return "";
        }
    }

    /**
     * Gets the column descriptor list
     *
     * @return The column descriptor list for this table descriptor
     */
    public ColumnDescriptorList getColumnDescriptorList(){
        return columnDescriptorList;
    }

    /**
     * Gets the list of columns defined by generation clauses.
     */
    public ColumnDescriptorList getGeneratedColumns(){
        ColumnDescriptorList fullList=getColumnDescriptorList();
        ColumnDescriptorList result=new ColumnDescriptorList();
        int count=fullList.size();

        for(int i=0;i<count;i++){
            ColumnDescriptor cd=fullList.elementAt(i);
            if(cd.hasGenerationClause()){
                result.add(oid,cd);
            }
        }

        return result;
    }

    /**
     * Turn an array of column names into the corresponding 1-based column positions.
     */
    public int[] getColumnIDs(String[] names){
        int count=names.length;
        int[] result=new int[count];

        for(int i=0;i<count;i++){
            result[i]=getColumnDescriptor(names[i]).getPosition();
        }

        return result;
    }

    /**
     * Gets the constraint descriptor list
     *
     * @return The constraint descriptor list for this table descriptor
     * @throws StandardException Thrown on failure
     */
    public ConstraintDescriptorList getConstraintDescriptorList() throws StandardException{
        return constraintDescriptorList;
    }

    /**
     * Sets the constraint descriptor list
     *
     * @param newCDL The new constraint descriptor list for this table descriptor
     */
    public void setConstraintDescriptorList(ConstraintDescriptorList newCDL){
        constraintDescriptorList=newCDL;
    }

    /**
     * Empty the constraint descriptor list
     *
     * @throws StandardException Thrown on failure
     */
    public void emptyConstraintDescriptorList() throws StandardException{
        // Easier just to get a new CDL then to clean out the current one
        this.constraintDescriptorList=new ConstraintDescriptorList();
    }

    /**
     * Gets the primary key, may return null if no primary key
     *
     * @return The priamry key or null
     * @throws StandardException Thrown on failure
     */
    public ReferencedKeyConstraintDescriptor getPrimaryKey() throws StandardException{
        ConstraintDescriptorList cdl=getDataDictionary().getConstraintDescriptors(this);

        return cdl.getPrimaryKey();
    }

    /**
     * Gets the trigger descriptor list
     *
     * @return The trigger descriptor list for this table descriptor
     * @throws StandardException Thrown on failure
     */
    public GenericDescriptorList getTriggerDescriptorList() throws StandardException{
        return triggerDescriptorList;
    }

    /**
     * Sets the trigger descriptor list
     *
     * @param newCDL The new trigger descriptor list for this table descriptor
     */
    public void setTriggerDescriptorList(GenericDescriptorList newCDL){
        triggerDescriptorList=newCDL;
    }

    /**
     * Empty the trigger descriptor list
     *
     * @throws StandardException Thrown on failure
     */
    public void emptyTriggerDescriptorList() throws StandardException{
        // Easier just to get a new CDL then to clean out the current one
        this.triggerDescriptorList=new GenericDescriptorList();
    }


    /**
     * Compare the tables descriptors based on the names.
     * Null schema names match.
     *
     * @param otherTableName  the other table name
     * @param otherSchemaName the other schema name
     * @return boolean        Whether or not the 2 TableNames are equal.
     */
    public boolean tableNameEquals(String otherTableName,String otherSchemaName){
        String schemaName=getSchemaName();

        if((schemaName==null) ||
                (otherSchemaName==null)){
            return tableName.equals(otherTableName);
        }else{
            return schemaName.equals(otherSchemaName) &&
                    tableName.equals(otherTableName);
        }
    }

    /**
     * Remove this descriptor
     *
     * @param cd The conglomerate descriptor
     * @throws StandardException on error
     */
    public void removeConglomerateDescriptor(ConglomerateDescriptor cd) throws StandardException{
        conglomerateDescriptorList.dropConglomerateDescriptor(getUUID(), cd);
    }

    /**
     * Remove this descriptor.  Warning, removes by using object
     * reference, not uuid.
     *
     * @param cd constraint descriptor
     * @throws StandardException on error
     */
    public void removeConstraintDescriptor(ConstraintDescriptor cd) throws StandardException{
        constraintDescriptorList.remove(cd);
    }

    /**
     * Get the descriptor for a column in the table,
     * either by the column name or by its ordinal position (column number).
     * Returns NULL for columns that do not exist.
     *
     * @param columnName A String containing the name of the column
     * @return A ColumnDescriptor describing the column
     */
    public ColumnDescriptor getColumnDescriptor(String columnName){
        return columnDescriptorList.getColumnDescriptor(oid,columnName);
    }

    /**
     * @param columnNumber The ordinal position of the column in the table
     * @return A ColumnDescriptor describing the column
     */
    public ColumnDescriptor getColumnDescriptor(int columnNumber){
        return columnDescriptorList.getColumnDescriptor(oid,columnNumber);
    }

    /**
     * Gets a ConglomerateDescriptor[] to loop through all the conglomerate descriptors
     * for the table.
     *
     * @return A ConglomerateDescriptor[] for looping through the table's conglomerates
     */
    public ConglomerateDescriptor[] getConglomerateDescriptors(){

        int size=conglomerateDescriptorList.size();
        ConglomerateDescriptor[] cdls=new ConglomerateDescriptor[size];
        conglomerateDescriptorList.toArray(cdls);
        return cdls;
    }

    /**
     * Gets a conglomerate descriptor for the given table and conglomerate number.
     *
     * @param conglomerateNumber The conglomerate number
     *                           we're interested in
     * @return A ConglomerateDescriptor describing the requested
     * conglomerate. Returns NULL if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    public ConglomerateDescriptor getConglomerateDescriptor(long conglomerateNumber) throws StandardException{
        return conglomerateDescriptorList.getConglomerateDescriptor(conglomerateNumber);
    }

    /**
     * Gets array of conglomerate descriptors for the given table and
     * conglomerate number.  More than one descriptors if duplicate indexes
     * share one conglomerate.
     *
     * @param conglomerateNumber The conglomerate number
     *                           we're interested in
     * @return Array of ConglomerateDescriptors with the requested
     * conglomerate number. Returns size 0 array if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    public ConglomerateDescriptor[] getConglomerateDescriptors(long conglomerateNumber) throws StandardException{
        return conglomerateDescriptorList.getConglomerateDescriptors(conglomerateNumber);
    }


    /**
     * Gets a conglomerate descriptor for the given table and conglomerate UUID String.
     *
     * @param conglomerateUUID The UUID  for the conglomerate
     *                         we're interested in
     * @return A ConglomerateDescriptor describing the requested
     * conglomerate. Returns NULL if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    public ConglomerateDescriptor getConglomerateDescriptor(UUID conglomerateUUID) throws StandardException{
        return conglomerateDescriptorList.getConglomerateDescriptor(conglomerateUUID);
    }

    /**
     * Gets array of conglomerate descriptors for the given table and
     * conglomerate UUID.  More than one descriptors if duplicate indexes
     * share one conglomerate.
     *
     * @param conglomerateUUID The conglomerate UUID
     *                         we're interested in
     * @return Array of ConglomerateDescriptors with the requested
     * conglomerate UUID. Returns size 0 array if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    public ConglomerateDescriptor[] getConglomerateDescriptors(UUID conglomerateUUID) throws StandardException{
        return conglomerateDescriptorList.getConglomerateDescriptors(conglomerateUUID);
    }

    /**
     * Gets an object which lists out all the index row generators on a table together
     * with their conglomerate ids.
     *
     * @return An object to list out the index row generators.
     * @throws StandardException Thrown on failure
     */
    public IndexLister getIndexLister() throws StandardException{
        return new IndexLister(this);
    }

    /**
     * Does the table have an autoincrement column or not?
     *
     * @return TRUE if the table has atleast one autoincrement column, false
     * otherwise
     */
    public boolean tableHasAutoincrement(){
        int cdlSize=getColumnDescriptorList().size();
        for(int index=0;index<cdlSize;index++){
            ColumnDescriptor cd=
                    columnDescriptorList.elementAt(index);
            if(cd.isAutoincrement())
                return true;
        }
        return false;
    }

    /**
     * Gets an array of column names.
     *
     * @return An array, filled with the column names in the table.
     */
    public String[] getColumnNamesArray(){
        int size=getNumberOfColumns();
        String[] s=new String[size];

        for(int i=0;i<size;i++)
            s[i]=getColumnDescriptor(i+1).getColumnName();

        return s;
    }

    /**
     * gets an array of increment values for autoincrement columns in the target
     * table. If column is not an autoincrement column, then increment value is
     * 0. If table has no autoincrement columns, returns NULL.
     *
     * @return array containing the increment values of autoincrement
     * columns.
     */
    public long[] getAutoincIncrementArray(){
        if(!tableHasAutoincrement())
            return null;

        int size=getNumberOfColumns();
        long[] inc=new long[size];

        for(int i=0;i<size;i++){
            ColumnDescriptor cd=getColumnDescriptor(i+1);
            if(cd.isAutoincrement())
                inc[i]=cd.getAutoincInc();
        }

        return inc;
    }


    /**
     * @see TupleDescriptor#getDescriptorName
     */
    public String getDescriptorName(){
        return tableName;
    }

    /**
     * @see TupleDescriptor#getDescriptorType
     */
    public String getDescriptorType(){
        return (tableType==TableDescriptor.SYNONYM_TYPE)?"Synonym":"Table/View";
    }

    //////////////////////////////////////////////////////
    //
    // DEPENDENT INTERFACE
    //
    //////////////////////////////////////////////////////

    /**
     * Check that all of the dependent's dependencies are valid.
     *
     * @return true if the dependent is currently valid
     */
    public synchronized boolean isValid(){
        return true;
    }

    /**
     * Prepare to mark the dependent as invalid (due to at least one of
     * its dependencies being invalid).
     *
     * @param action The action causing the invalidation
     * @param p      the provider
     * @throws StandardException thrown if unable to make it invalid
     */
    public void prepareToInvalidate(Provider p,int action,
                                    LanguageConnectionContext lcc)
            throws StandardException{
        DependencyManager dm=getDataDictionary().getDependencyManager();

        switch(action){
			/*
			** Currently, the only thing we are dependent
			** on is an alias descriptor for an ANSI UDT.
			*/
            default:

                throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_TABLE,
                        dm.getActionString(action),
                        p.getObjectName(),
                        getQualifiedName());
        }
    }

    /**
     * Mark the dependent as invalid (due to at least one of
     * its dependencies being invalid).  Always an error
     * for a table -- should never have gotten here.
     *
     * @param action The action causing the invalidation
     * @throws StandardException thrown if called in sanity mode
     */
    public void makeInvalid(int action,LanguageConnectionContext lcc)
            throws StandardException{
		/* 
		** We should never get here, we should have barfed on 
		** prepareToInvalidate().
		*/
        if(SanityManager.DEBUG){
            DependencyManager dm;

            dm=getDataDictionary().getDependencyManager();

            SanityManager.THROWASSERT("makeInvalid("+
                    dm.getActionString(action)+
                    ") not expected to get called");
        }
    }

    public void setSchemaDesctiptor(SchemaDescriptor schemaDesctiptor){
        this.schemaDesctiptor=schemaDesctiptor;
    }

    public ConglomerateDescriptor getBaseConglomerateDescriptor() {
        return getConglomerateDescriptorList()==null?null:getConglomerateDescriptorList().getBaseConglomerateDescriptor();
    }

    /**
     * Retrieve the formatIds from the TableDescriptor
     *
     * @return
     * @throws StandardException
     */
    public int[] getFormatIds() throws StandardException {
        return columnDescriptorList.getFormatIds();
    }

    /**
     *
     * Retrieves the columnOrdering
     *
     * @param dd
     * @return
     */
    public int[] getColumnOrdering(DataDictionary dd) throws StandardException {
        return dd.getConstraintDescriptors(this).getBaseColumnOrdering();
    }

    public ColumnInfo[] getColumnInfo() {
        int len = columnDescriptorList.size();
        ColumnInfo[] columnInfo = new ColumnInfo[len];
        for (int i = 0; i < len; ++i) {
            ColumnDescriptor desc = columnDescriptorList.get(i);
            columnInfo[i] =
                    new ColumnInfo(desc.getColumnName(),
                            desc.getType(),
                            desc.getDefaultValue(),
                            desc.getDefaultInfo(),
                            null,
                            desc.getDefaultUUID(),
                            null,
                            0,
                            desc.getAutoincStart(),
                            desc.getAutoincInc(),
                            desc.getAutoinc_create_or_modify_Start_Increment(),
                            desc.getPartitionPosition());
        }
        return columnInfo;
    }

    public int[] getPartitionBy() {
        int length = columnDescriptorList.size();
        TreeMap<Integer,Integer> map = null;
        for (int i =0; i< length;i++) {
            ColumnDescriptor desc = columnDescriptorList.get(i);
            if (desc.getPartitionPosition() !=-1) {
                if (map==null)
                    map = new TreeMap<>();
                map.put(desc.getPartitionPosition(),i);
            }
        }
        if (map==null)
            return EMPTY_PARTITON_ARRAY;
        return Ints.toArray(map.values());
    }

    public boolean isExternal() {
        return tableType == EXTERNAL_TYPE;
    }

        /*
         * Get an int[] mapping the column position of the indexed columns in the main table
         * to it's location in the index table. The values are placed in the provided baseColumnPositions array,
         * and the highest baseColumnPosition is returned.
         */
    public int getBaseColumnPositions(LanguageConnectionContext lcc,
                                       int[] baseColumnPositions, String[] columnNames) throws StandardException {
        int maxBaseColumnPosition = Integer.MIN_VALUE;
        ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
        for (int i = 0; i < columnNames.length; i++) {
            // Look up the column in the data dictionary --main table column
            ColumnDescriptor columnDescriptor = getColumnDescriptor(columnNames[i]);
            if (columnDescriptor == null) {
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, columnNames[i],tableName);
            }

            TypeId typeId = columnDescriptor.getType().getTypeId();

            // Don't allow a column to be created on a non-orderable type
            boolean isIndexable = typeId.orderable(cf);

            if (isIndexable && typeId.userType()) {
                String userClass = typeId.getCorrespondingJavaTypeName();

                // Don't allow indexes to be created on classes that
                // are loaded from the database. This is because recovery
                // won't be able to see the class and it will need it to
                // run the compare method.
                try {
                    if (cf.isApplicationClass(cf.loadApplicationClass(userClass)))
                        isIndexable = false;
                } catch (ClassNotFoundException cnfe) {
                    // shouldn't happen as we just check the class is orderable
                    isIndexable = false;
                }
            }

            if (!isIndexable)
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, typeId.getSQLTypeName());

            // Remember the position in the base table of each column
            baseColumnPositions[i] = columnDescriptor.getPosition();

            if (maxBaseColumnPosition < baseColumnPositions[i])
                maxBaseColumnPosition = baseColumnPositions[i];
        }
        return maxBaseColumnPosition;
    }

    public DataValueDescriptor getDefaultValue(int columnNumber) {
        return getColumnDescriptor(columnNumber).getDefaultValue();
    }

}

