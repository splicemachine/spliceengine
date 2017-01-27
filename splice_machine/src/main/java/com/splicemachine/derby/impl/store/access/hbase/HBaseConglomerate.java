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

package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.store.access.*;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.ScanManager;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.store.access.conglomerate.ConglomerateUtil;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructField;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

/**
 * A hbase object corresponds to an instance of a hbase conglomerate.
 **/

public class HBaseConglomerate extends SpliceConglomerate{
    public static final long serialVersionUID=5l;
    private static final Logger LOG=Logger.getLogger(HBaseConglomerate.class);

    public HBaseConglomerate(){
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
            TxnOperationFactory operationFactory,
            PartitionFactory partitionFactory) throws StandardException{
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
        String partitionSizeStr=properties.getProperty("partitionSize");
        long pSize = -1l;
        if(partitionSizeStr!=null){
            try{
                pSize=Long.parseLong(partitionSizeStr);
            }catch(NumberFormatException nfe){
               //TODO -sf- add a warning to the activation that we weren't able to
            }
        }
        ConglomerateUtils.createConglomerate(isExternal,
                containerId,
                this,
                ((SpliceTransaction)rawtran).getTxn(),
                properties.getProperty(SIConstants.TABLE_DISPLAY_NAME_ATTR),
                properties.getProperty(SIConstants.INDEX_DISPLAY_NAME_ATTR),
                pSize);
    }

    @Override
    public void dropColumn(TransactionManager xact_manager,int column_id) throws StandardException{
        SpliceLogUtils.trace(LOG,"dropColumn column_id=%s, table_nam=%s",column_id,getContainerid());
        try{
            format_ids=ConglomerateUtils.dropValueFromArray(format_ids,column_id-1);
            collation_ids=ConglomerateUtils.dropValueFromArray(collation_ids,column_id-1);
            ConglomerateUtils.updateConglomerate(this,(Txn)((SpliceTransactionManager)xact_manager).getActiveStateTxn());
        }catch(StandardException e){
            SpliceLogUtils.logAndThrow(LOG,"exception in HBaseConglomerate#addColumn",e);
        }finally{
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
    public void addColumn(TransactionManager xact_manager,int column_id,Storable template_column,int collation_id) throws StandardException{
        SpliceLogUtils.trace(LOG,"addColumn column_id=%s, template_column=%s, table_nam=%s",column_id,template_column,getContainerid());
        try{
            int[] old_format_ids=format_ids;
            format_ids=new int[old_format_ids.length+1];
            System.arraycopy(old_format_ids,0,format_ids,0,old_format_ids.length);

            // add the new column
            format_ids[old_format_ids.length]=template_column.getTypeFormatId();

            // create a new collation array, and copy old values to it.
            int[] old_collation_ids=collation_ids;
            collation_ids=new int[old_collation_ids.length+1];
            System.arraycopy(old_collation_ids,0,collation_ids,0,old_collation_ids.length);
            // add the new column's collation id.
            collation_ids[old_collation_ids.length]=collation_id;
            ConglomerateUtils.updateConglomerate(this,(Txn)((SpliceTransactionManager)xact_manager).getActiveStateTxn());
        }catch(StandardException e){
            SpliceLogUtils.logAndThrow(LOG,"exception in HBaseConglomerate#addColumn",e);
        }
    }


    /**
     * Drop this hbase conglomerate (what's the relationship with dropping container).
     *
     * @throws StandardException Standard exception policy.
     * @see Conglomerate#drop
     **/
    public void drop(TransactionManager xact_manager) throws StandardException{
        SpliceLogUtils.trace(LOG,"drop with account manager %s",xact_manager);
        try(PartitionAdmin pa=partitionFactory.getAdmin()){
            pa.deleteTable(Long.toString(containerId));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
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
    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo()
            throws StandardException{
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
        return (this);
    }


    /**
     * Bulk load into the conglomerate.
     * <p/>
     *
     * @throws StandardException Standard exception policy.
     * @see Conglomerate#load
     **/
    public long load(TransactionManager xact_manager,
                     boolean createConglom,
                     RowLocationRetRowSource rowSource) throws StandardException{
        SpliceLogUtils.trace(LOG,"load rowSourc %s",rowSource);
        return 0l;
    }

    /**
     * Open a hbase controller.
     * <p/>
     *
     * @throws StandardException Standard exception policy.
     * @see Conglomerate#open
     **/
    @Override
    public ConglomerateController open(TransactionManager xact_manager,
                                       Transaction rawtran,
                                       boolean hold,
                                       int open_mode,
                                       int lock_level,
                                       StaticCompiledOpenConglomInfo static_info,
                                       DynamicCompiledOpenConglomInfo dynamic_info) throws StandardException{
        SpliceLogUtils.trace(LOG,"open conglomerate id: %d",containerId);
        OpenSpliceConglomerate open_conglom=new OpenSpliceConglomerate(xact_manager,rawtran,hold,static_info,dynamic_info,this);
        return new HBaseController(open_conglom,rawtran,partitionFactory,opFactory);
    }

    /**
     * Open a hbase scan controller.
     * <p/>
     *
     * @throws StandardException Standard exception policy.
     * @see Conglomerate#openScan
     **/
    public ScanManager openScan(
            TransactionManager xact_manager,
            Transaction rawtran,
            boolean hold,
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
            DynamicCompiledOpenConglomInfo dynamic_info)
            throws StandardException{
        SpliceLogUtils.trace(LOG,"open scan: %s",containerId);
        if(!RowUtil.isRowEmpty(startKeyValue) || !RowUtil.isRowEmpty(stopKeyValue))
            throw StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE);
        OpenSpliceConglomerate open_conglom=new OpenSpliceConglomerate(xact_manager,rawtran,hold,static_info,dynamic_info,this);
        return new SpliceScan(open_conglom,scanColumnList,startKeyValue,startSearchOperator,
                qualifier,stopKeyValue,stopSearchOperator,rawtran,false,opFactory,partitionFactory);
    }

    public void purgeConglomerate(TransactionManager xact_manager,Transaction rawtran) throws StandardException{
        SpliceLogUtils.trace(LOG,"purgeConglomerate: %s",containerId);
    }

    public void compressConglomerate(TransactionManager xact_manager,Transaction rawtran) throws StandardException{
        SpliceLogUtils.trace(LOG,"compressConglomerate: %s",containerId);
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
     **************************************************************************
     */

    /**
     * Return my format identifier.
     *
     * @see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
     **/
    public int getTypeFormatId(){
        return StoredFormatIds.ACCESS_HEAP_V3_ID;
    }

    /**
     * Store the stored representation of column value in stream.
     * <p/>
     * This routine uses the current database version to either store the
     * the 10.2 format (ACCESS_HEAP_V2_ID) or the current format
     * (ACCESS_HEAP_V3_ID).
     * <p/>
     **/
    public void writeExternal(ObjectOutput out) throws IOException{
        FormatIdUtil.writeFormatIdInteger(out,conglom_format_id);
        FormatIdUtil.writeFormatIdInteger(out,tmpFlag);
        out.writeLong(containerId);
        out.writeInt(format_ids.length);
        ConglomerateUtil.writeFormatIdArray(format_ids,out);
        out.writeInt(collation_ids.length);
        ConglomerateUtil.writeFormatIdArray(collation_ids,out);
        out.writeInt(columnOrdering.length);
        ConglomerateUtil.writeFormatIdArray(columnOrdering,out);
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
    private void localReadExternal(ObjectInput in)
            throws IOException, ClassNotFoundException{
//        SpliceLogUtils.trace(LOG,"localReadExternal");
        // read the format id of this conglomerate.
        conglom_format_id=FormatIdUtil.readFormatIdInteger(in);
        tmpFlag=FormatIdUtil.readFormatIdInteger(in);
        containerId=in.readLong();
        // read the number of columns in the heap.
        int num_columns=in.readInt();
        // read the array of format ids.
        format_ids=ConglomerateUtil.readFormatIdArray(num_columns,in);
        this.conglom_format_id=getTypeFormatId();
        num_columns=in.readInt();
        collation_ids=ConglomerateUtil.readFormatIdArray(num_columns,in);
        num_columns=in.readInt();
        columnOrdering=ConglomerateUtil.readFormatIdArray(num_columns,in);

        partitionFactory=SIDriver.driver().getTableFactory();
        opFactory=SIDriver.driver().getOperationFactory();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        localReadExternal(in);
    }


    public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException{
        if(LOG.isTraceEnabled())
            LOG.trace("readExternalFromArray: ");
        localReadExternal(in);
    }

    @Override
    public int getBaseMemoryUsage(){
        return ClassSize.estimateBaseFromCatalog(HBaseConglomerate.class);
    }

    @Override
    public StructField getStructField(String columnName) {
        throw new RuntimeException("Not Implemented");
    }

}
