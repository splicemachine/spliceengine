
/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.store.access.btree;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.store.access.*;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.ScanManager;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.impl.store.access.conglomerate.ConglomerateUtil;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructField;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

/**
 * An index object corresponds to an instance of a btree conglomerate.
 **/


public class IndexConglomerate extends SpliceConglomerate{
    private static final Logger LOG=Logger.getLogger(IndexConglomerate.class);
    public static final int FORMAT_NUMBER=StoredFormatIds.ACCESS_B2I_V6_ID;
    public static final String PROPERTY_UNIQUE_WITH_DUPLICATE_NULLS="uniqueWithDuplicateNulls";
    private static final long serialVersionUID=4l;
    protected static final String PROPERTY_BASECONGLOMID="baseConglomerateId";
    protected static final String PROPERTY_ROWLOCCOLUMN="rowLocationColumn";
    protected static final String PROPERTY_ALLOWDUPLICATES="allowDuplicates";
    protected static final String PROPERTY_NKEYFIELDS="nKeyFields";
    protected static final String PROPERTY_NUNIQUECOLUMNS="nUniqueColumns";
    protected static final String PROPERTY_PARENTLINKS="maintainParentLinks";
    protected static int BASE_MEMORY_USAGE=ClassSize.estimateBaseFromCatalog(IndexConglomerate.class);

    public long baseConglomerateId;
    protected int rowLocationColumn;
    protected boolean[] ascDescInfo;
    protected int nKeyFields;
    protected int nUniqueColumns;
    protected boolean allowDuplicates;
    protected boolean maintainParentLinks;
    protected boolean uniqueWithDuplicateNulls=false;


    public IndexConglomerate(){
        super();
    }

    protected void create(boolean isExternal,
                          Transaction rawtran,
                          long input_containerid,
                          DataValueDescriptor[] template,
                          ColumnOrdering[] columnOrder,
                          int[] collationIds,
                          Properties properties,
                          int conglom_format_id,
                          int tmpFlag,
                          TxnOperationFactory opFactory,
                          PartitionFactory partitionFactory,
                          byte[][] splitKeys) throws StandardException{
        super.create(isExternal,rawtran,
                input_containerid,
                template,
                columnOrder,
                collationIds,
                properties,
                conglom_format_id,
                tmpFlag,
                opFactory,
                partitionFactory);
        if(properties==null)
            throw (StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND,PROPERTY_BASECONGLOMID));
        String property_value=properties.getProperty(PROPERTY_BASECONGLOMID);
        if(property_value==null)
            throw (StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND,PROPERTY_BASECONGLOMID));
        baseConglomerateId=Long.parseLong(property_value);
        property_value=properties.getProperty(PROPERTY_ROWLOCCOLUMN);
        if(SanityManager.DEBUG){
            if(property_value==null)
                SanityManager.THROWASSERT(PROPERTY_ROWLOCCOLUMN+"property not passed to B2I.create()");
        }
        rowLocationColumn=Integer.parseInt(property_value);
        // Currently the row location column must be the last column (makes) comparing the columns in the index easier.
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(rowLocationColumn==template.length-1,"rowLocationColumn is not the last column in the index");
            SanityManager.ASSERT(template[rowLocationColumn] instanceof RowLocation);
            // There must be at least one key column
            if(rowLocationColumn<1)
                SanityManager.THROWASSERT("rowLocationColumn ("+rowLocationColumn+") expected to be >= 1");
        }

        // Check input arguments
        allowDuplicates=Boolean.valueOf(properties.getProperty(PROPERTY_ALLOWDUPLICATES,"false"));

        property_value=properties.getProperty(PROPERTY_NKEYFIELDS);
        if(property_value==null)
            throw (StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND,PROPERTY_NKEYFIELDS));
        nKeyFields=Integer.parseInt(property_value);
        property_value=properties.getProperty(PROPERTY_NUNIQUECOLUMNS);
        if(property_value==null)
            throw (StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND,PROPERTY_NUNIQUECOLUMNS));
        nUniqueColumns=Integer.parseInt(property_value);

        ascDescInfo=new boolean[nUniqueColumns];
        for(int i=0;i<ascDescInfo.length;i++){
            if(columnOrder!=null && i<columnOrder.length)
                ascDescInfo[i]=columnOrder[i].getIsAscending();
            else
                ascDescInfo[i]=true;  // default values - ascending order
        }

        property_value=properties.getProperty(PROPERTY_UNIQUE_WITH_DUPLICATE_NULLS,"false");
        uniqueWithDuplicateNulls=Boolean.valueOf(property_value);
        maintainParentLinks=Boolean.valueOf(properties.getProperty(PROPERTY_PARENTLINKS,"true"));

        if(SanityManager.DEBUG){
            SanityManager.ASSERT((nUniqueColumns==nKeyFields) || (nUniqueColumns==(nKeyFields-1)));
        }
        try{
//            ((SpliceTransaction)rawtran).elevate(Bytes.toBytes(Long.toString(containerId)));
            ConglomerateUtils.createConglomerate(isExternal,
                    containerId,
                    this,
                    ((SpliceTransaction)rawtran).getTxn(),
                    properties.getProperty(SIConstants.SCHEMA_DISPLAY_NAME_ATTR),
                    properties.getProperty(SIConstants.TABLE_DISPLAY_NAME_ATTR),
                    properties.getProperty(SIConstants.INDEX_DISPLAY_NAME_ATTR),
                    splitKeys);
        }catch(Exception e){
            LOG.error(e.getMessage(),e);
        }
    }

	/*
    ** Methods of Conglomerate
	*/

    /**
     * Add a column to the hbase conglomerate.
     * <p/>
     * This routine update's the in-memory object version of the HBase
     * Conglomerate to have one more column of the type described by the
     * input template column.
     *
     * @param column_id       The column number to add this column at.
     * @param template_column An instance of the column to be added to table.
     * @param collation_id    Collation id of the column added.
     * @throws StandardException Standard exception policy.
     **/
    public void addColumn(
            TransactionManager xact_manager,
            int column_id,
            Storable template_column,
            int collation_id)
            throws StandardException{
        throw StandardException.newException(SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }


    /**
     * Drops a column from the hbase conglomerate.
     * <p>
     * This routine update's the in-memory object version of the HBase
     * Conglomerate to have one less column
     *
     * @param column_id        The column number to add this column at.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void dropColumn(
            TransactionManager  xact_manager,
            int                 column_id)
            throws StandardException {
        throw StandardException.newException(SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }


    /**
     * Return dynamic information about the conglomerate to be dynamically
     * reused in repeated execution of a statement.
     * <p/>
     * The dynamic info is a set of variables to be used in a given
     * ScanController or ConglomerateController.  It can only be used in one
     * controller at a time.  It is up to the caller to insure the correct
     * thread access to this info.  The type of info in this is a scratch
     * template for btree traversal, other scratch variables for qualifier
     * evaluation, ...
     * <p/>
     *
     * @return The dynamic information.
     * @throws StandardException Standard exception policy.
     **/
    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo() throws StandardException{
        if(LOG.isTraceEnabled())
            LOG.trace("getDynamicCompiledConglomInfo ");
        //FIXME: do we need this
        return null;
//        return (new OpenConglomerateScratchSpace(format_ids,collation_ids,hasCollatedTypes));
    }

    /**
     * Return static information about the conglomerate to be included in a
     * a compiled plan.
     * <p/>
     * The static info would be valid until any ddl was executed on the
     * conglomid, and would be up to the caller to throw away when that
     * happened.  This ties in with what language already does for other
     * invalidation of static info.  The type of info in this would be
     * containerid and array of format id's from which templates can be created.
     * The info in this object is read only and can be shared among as many
     * threads as necessary.
     * <p/>
     *
     * @param conglomId The identifier of the conglomerate to open.
     * @return The static compiled information.
     * @throws StandardException Standard exception policy.
     **/
    public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(TransactionController tc,
                                                                      long conglomId) throws StandardException{
        return this;
    }


    /**
     * Bulk load into the conglomerate.
     * <p/>
     *
     * @throws StandardException Standard exception policy.
     * @see Conglomerate#load
     **/
    public long load(TransactionManager xact_manager,boolean createConglom,
                     RowLocationRetRowSource rowSource) throws StandardException{
        return 0;
    }


    public ConglomerateController open(
            TransactionManager xact_manager,
            Transaction rawtran,
            boolean hold,
            int open_mode,
            int lock_level,
            StaticCompiledOpenConglomInfo static_info,
            DynamicCompiledOpenConglomInfo dynamic_info) throws StandardException{
        SpliceLogUtils.trace(LOG,"open conglomerate id: %s",containerId);
        OpenSpliceConglomerate open_conglom=new OpenSpliceConglomerate(xact_manager,rawtran,hold,static_info,dynamic_info,this);
        return new IndexController(open_conglom,rawtran,partitionFactory,opFactory,nUniqueColumns);
    }

    /**
     * Open a hbase scan controller.
     * <p/>
     *
     * @throws StandardException Standard exception policy.
     * @see Conglomerate#openScan
     **/
    public ScanManager openScan(TransactionManager xact_manager,Transaction rawtran,boolean hold,
                                int open_mode,
                                int lock_level,
                                int isolation_level,
                                FormatableBitSet scanColumnList,
                                DataValueDescriptor[] startKeyValue,
                                int startSearchOperator,
                                Qualifier qualifier[][],
                                DataValueDescriptor[] stopKeyValue,
                                int stopSearchOperator,
                                StaticCompiledOpenConglomInfo static_info,
                                DynamicCompiledOpenConglomInfo dynamic_info) throws StandardException{
        OpenSpliceConglomerate open_conglom=new OpenSpliceConglomerate(xact_manager,
                rawtran,hold,
                static_info,
                dynamic_info,this);
        DataValueDescriptor[] uniqueStartKey=rowKeyForUniqueFields(startKeyValue);
        DataValueDescriptor[] uniqueStopKey=rowKeyForUniqueFields(stopKeyValue);
        return new SpliceScan(open_conglom,
                scanColumnList,
                uniqueStartKey,
                startSearchOperator,
                qualifier,
                uniqueStopKey,
                stopSearchOperator,
                rawtran,
                true,
                opFactory,
                partitionFactory);
    }

    private DataValueDescriptor[] rowKeyForUniqueFields(DataValueDescriptor[] rowKey){
        if(rowKey==null || rowKey.length<=nUniqueColumns){
            return rowKey;
        }
        DataValueDescriptor[] uniqueRowKey=new DataValueDescriptor[nUniqueColumns];
        System.arraycopy(rowKey,0,uniqueRowKey,0,nUniqueColumns);
        return uniqueRowKey;
    }

    public void purgeConglomerate(
            TransactionManager xact_manager,
            Transaction rawtran) throws StandardException{
        SpliceLogUtils.trace(LOG,"purgeConglomerate: %s",containerId);
    }

    public void compressConglomerate(
            TransactionManager xact_manager,
            Transaction rawtran) throws StandardException{
        SpliceLogUtils.trace(LOG,"compressConglomerate: %s",containerId);
    }

    /**
     * Print this hbase.
     **/
    public String toString(){
        return String.format("IndexConglomerate {id=%s, baseConglomerateId=%d}",containerId==-1l?"null":""+containerId,baseConglomerateId);
    }

    /**************************************************************************
     * Public Methods of StaticCompiledOpenConglomInfo Interface:
     **************************************************************************
     */

    /**
     * return the "Conglomerate".
     * <p/>
     * For hbase just return "this", which both implements Conglomerate and
     * StaticCompiledOpenConglomInfo.
     * <p/>
     *
     * @return this
     **/
    public DataValueDescriptor getConglom(){
        SpliceLogUtils.trace(LOG,"getConglom: %s",containerId);
        return (this);
    }


    /**************************************************************************
     * Methods of Storable (via Conglomerate)
     * Storable interface, implies Externalizable, TypedFormat
     * *************************************************************************
     */

    public int getTypeFormatId(){
        return StoredFormatIds.ACCESS_B2I_V6_ID;
    }


    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p/>
     * For more detailed description of the ACCESS_B2I_V3_ID format see
     * documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
    public void writeExternal_v10_2(ObjectOutput out) throws IOException{
        if(LOG.isTraceEnabled())
            LOG.trace("writeExternal_v10_2");
        btreeWriteExternal(out);
        out.writeLong(baseConglomerateId);
        out.writeInt(rowLocationColumn);

        //write the columns ascend/descend information as bits
        FormatableBitSet ascDescBits=
                new FormatableBitSet(ascDescInfo.length);

        for(int i=0;i<ascDescInfo.length;i++){
            if(ascDescInfo[i])
                ascDescBits.set(i);
        }
        ascDescBits.writeExternal(out);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public boolean[] getAscDescInfo(){
        return ascDescInfo;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void setAscDescInfo(boolean[] ascDescInfo){
        this.ascDescInfo=ascDescInfo;
    }

    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p/>
     * For more detailed description of the ACCESS_B2I_V3_ID and
     * ACCESS_B2I_V4_ID formats see documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
    public void writeExternal_v10_3(ObjectOutput out) throws IOException{
        if(LOG.isTraceEnabled())
            LOG.trace("writeExternal_v10_3");

        // First part of ACCESS_B2I_V4_ID format is the ACCESS_B2I_V3_ID format.
        writeExternal_v10_2(out);
        if(conglom_format_id==StoredFormatIds.ACCESS_B2I_V4_ID
                || conglom_format_id==StoredFormatIds.ACCESS_B2I_V5_ID
                || conglom_format_id==StoredFormatIds.ACCESS_B2I_V6_ID){
            // Now append sparse array of collation ids
            ConglomerateUtil.writeCollationIdArray(collation_ids,out);
        }
    }


    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p/>
     * For more detailed description of the ACCESS_B2I_V3_ID and
     * ACCESS_B2I_V5_ID formats see documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
    public void writeExternal(ObjectOutput out) throws IOException{
        if(LOG.isTraceEnabled())
            LOG.trace("writeExternal");
        writeExternal_v10_3(out);
        if(conglom_format_id==StoredFormatIds.ACCESS_B2I_V5_ID || conglom_format_id==StoredFormatIds.ACCESS_B2I_V6_ID)
            out.writeBoolean(isUniqueWithDuplicateNulls());
        int len=(columnOrdering!=null && columnOrdering.length>0)?columnOrdering.length:0;
        out.writeInt(len);
        if(len>0){
            ConglomerateUtil.writeFormatIdArray(columnOrdering,out);
        }
    }

    /**
     * Restore the in-memory representation from the stream.
     * <p/>
     *
     * @throws ClassNotFoundException Thrown if the stored representation
     *                                is serialized and a class named in
     *                                the stream could not be found.
     * @see java.io.Externalizable#readExternal
     **/
    private void localReadExternal(ObjectInput in) throws IOException, ClassNotFoundException{
//        SpliceLogUtils.trace(LOG,"localReadExternal");
        btreeReadExternal(in);
        baseConglomerateId=in.readLong();
        rowLocationColumn=in.readInt();
        // read the column sort order info
        FormatableBitSet ascDescBits=new FormatableBitSet();
        ascDescBits.readExternal(in);
        ascDescInfo=new boolean[ascDescBits.getLength()];
        for(int i=0;i<ascDescBits.getLength();i++)
            ascDescInfo[i]=ascDescBits.isSet(i);
        // In memory maintain a collation id per column in the template.
        collation_ids=new int[format_ids.length];
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(!hasCollatedTypes);
        }

        // initialize all the entries to COLLATION_TYPE_UCS_BASIC, 
        // and then reset as necessary.  For version ACCESS_B2I_V3_ID,
        // this is the default and no resetting is necessary.
        for(int i=0;i<format_ids.length;i++)
            collation_ids[i]=StringDataValue.COLLATION_TYPE_UCS_BASIC;

        // initialize the unique with null setting to false, to be reset
        // below when read from disk.  For version ACCESS_B2I_V3_ID and
        // ACCESS_B2I_V4_ID, this is the default and no resetting is necessary.
        setUniqueWithDuplicateNulls(false);
        if(conglom_format_id==StoredFormatIds.ACCESS_B2I_V4_ID
                || conglom_format_id==StoredFormatIds.ACCESS_B2I_V5_ID
                || conglom_format_id==StoredFormatIds.ACCESS_B2I_V6_ID){
            // current format id, read collation info from disk
            if(SanityManager.DEBUG){
                // length must include row location column and at least
                // one other field.
                SanityManager.ASSERT(
                        collation_ids.length>=2,
                        "length = "+collation_ids.length);
            }

            hasCollatedTypes=
                    ConglomerateUtil.readCollationIdArray(collation_ids,in);
        }else if(conglom_format_id!=StoredFormatIds.ACCESS_B2I_V3_ID){
            // Currently only V3, V4 and V5 should be possible in a Derby DB.
            // Actual work for V3 is handled by default code above, so no
            // special work is necessary.

            if(SanityManager.DEBUG){
                SanityManager.THROWASSERT(
                        "Unexpected format id: "+conglom_format_id);
            }
        }
        if(conglom_format_id==StoredFormatIds.ACCESS_B2I_V5_ID || conglom_format_id==StoredFormatIds.ACCESS_B2I_V6_ID){
            setUniqueWithDuplicateNulls(in.readBoolean());
        }
        int len=in.readInt();
        columnOrdering=ConglomerateUtil.readFormatIdArray(len,in);
        partitionFactory =SIDriver.driver().getTableFactory();
        opFactory = SIDriver.driver().getOperationFactory();
    }

    /**
     * Set if the index is unique only for non null keys
     *
     * @param uniqueWithDuplicateNulls true if the index will be unique only for
     *                                 non null keys
     */
    public void setUniqueWithDuplicateNulls(boolean uniqueWithDuplicateNulls){
        this.uniqueWithDuplicateNulls=uniqueWithDuplicateNulls;
    }

    /**
     * Returns if the index type is uniqueWithDuplicateNulls.
     *
     * @return is index type is uniqueWithDuplicateNulls
     */
    public boolean isUniqueWithDuplicateNulls(){
        return uniqueWithDuplicateNulls;
    }

    @Override
    public int getBaseMemoryUsage(){
        return ClassSize.estimateBaseFromCatalog(IndexConglomerate.class);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        if(LOG.isTraceEnabled())
            LOG.trace("readExternal");
        localReadExternal(in);
    }

    @Override
    public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException{
        if(LOG.isTraceEnabled())
            LOG.trace("readExternalFromArray");
        localReadExternal(in);
    }

    public void btreeWriteExternal(ObjectOutput out) throws IOException{
        FormatIdUtil.writeFormatIdInteger(out,conglom_format_id);
        FormatIdUtil.writeFormatIdInteger(out,tmpFlag);
        out.writeLong(containerId);
        out.writeInt((nKeyFields));
        out.writeInt((nUniqueColumns));
        out.writeBoolean((allowDuplicates));
        out.writeBoolean((maintainParentLinks));
        ConglomerateUtil.writeFormatIdArray(format_ids,out);
    }

    public void btreeReadExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        conglom_format_id=FormatIdUtil.readFormatIdInteger(in);
        tmpFlag=FormatIdUtil.readFormatIdInteger(in);
        containerId=in.readLong();
        nKeyFields=in.readInt();
        nUniqueColumns=in.readInt();
        allowDuplicates=in.readBoolean();
        maintainParentLinks=in.readBoolean();
        // read in the array of format id's
        format_ids=ConglomerateUtil.readFormatIdArray(this.nKeyFields,in);
    }

    @Override
    public StructField getStructField(String columnName) {
        throw new RuntimeException("Not Implemented");
    }

}
