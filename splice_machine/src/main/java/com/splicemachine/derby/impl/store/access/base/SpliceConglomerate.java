/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.raw.RawStoreFactory;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.store.access.conglomerate.ConglomerateUtil;
import com.splicemachine.db.impl.store.access.conglomerate.GenericConglomerate;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.si.api.data.TxnOperationFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;


public abstract class SpliceConglomerate extends GenericConglomerate implements Conglomerate, StaticCompiledOpenConglomInfo{
    private static final long serialVersionUID=7583841286945209190l;
    private static final Logger LOG=Logger.getLogger(SpliceConglomerate.class);
    protected int conglom_format_id;
    protected int tmpFlag;
    protected int[] format_ids;
    protected int[] collation_ids;
    protected int[] columnOrdering; // Primary Key Information
    protected boolean hasCollatedTypes;
    protected long nextContainerId=System.currentTimeMillis();
    protected long containerId = -1l;

    protected TxnOperationFactory opFactory;
    protected PartitionFactory partitionFactory;

    public SpliceConglomerate(){
    }

    protected void create(
            Transaction rawtran,
            long input_containerid,
            DataValueDescriptor[] template,
            ColumnOrdering[] columnOrder,
            int[] collationIds,
            Properties properties,
            int conglom_format_id,
            int tmpFlag,
            TxnOperationFactory opFactory,
            PartitionFactory partitionFactory) throws StandardException{
        this.opFactory = opFactory;
        this.partitionFactory = partitionFactory;
        if(properties!=null){
            String value=properties.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER);
            int minimumRecordSize=(value==null)?RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT:Integer.parseInt(value);
            if(minimumRecordSize<RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT){
                properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,Integer.toString(RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT));
            }
        }
        if(columnOrder!=null){
            columnOrdering=new int[columnOrder.length];
            for(int i=0;i<columnOrder.length;i++){
                columnOrdering[i]=columnOrder[i].getColumnId();
            }
        }else{
            columnOrdering=new int[0];
        }
        containerId=input_containerid;
        if((template==null) || (template.length==0)){
            throw StandardException.newException(SQLState.HEAP_COULD_NOT_CREATE_CONGLOMERATE);
        }

        this.format_ids=ConglomerateUtil.createFormatIds(template);
        this.conglom_format_id=conglom_format_id;
        collation_ids=ConglomerateUtil.createCollationIds(format_ids.length,collationIds);
        hasCollatedTypes=hasCollatedColumns(collation_ids);
        this.tmpFlag=tmpFlag;

        try{
            ((SpliceTransaction)rawtran).setActiveState(false,false,null);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public void boot_create(long containerid,DataValueDescriptor[] template){
        this.containerId = containerid;
        this.format_ids=ConglomerateUtil.createFormatIds(template);
    }

    synchronized long getNextId(){
        if(LOG.isTraceEnabled())
            LOG.trace("getNextId ");
        return nextContainerId++;
    }

    public int estimateMemoryUsage(){
        if(LOG.isTraceEnabled())
            LOG.trace("estimate Memory Usage");
        int sz=getBaseMemoryUsage();
        if(null!=format_ids)
            sz+=format_ids.length*ClassSize.getIntSize();
        return sz;
    }


    public final long getId(){
        if(LOG.isTraceEnabled())
            LOG.trace("getId ");
        return containerId;
    }

    public boolean[] getAscDescInfo(){
        return null;
    }

    public final long getContainerid(){
        return containerId;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getFormat_ids(){
        return format_ids;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getCollation_ids(){
        return collation_ids;
    }

    public boolean isNull(){
        return containerId == -1l;
    }

    /**
     * Is this conglomerate temporary?
     * <p/>
     *
     * @return whether conglomerate is temporary or not.
     **/
    public boolean isTemporary(){
        if(LOG.isTraceEnabled())
            LOG.trace("isTemporary ");
        return (tmpFlag&TransactionController.IS_TEMPORARY)==TransactionController.IS_TEMPORARY;
    }

    public void restoreToNull(){
        containerId=-1l;
    }

    public String toString(){
        return (containerId==-1l)?"null":""+containerId;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getColumnOrdering(){
        return columnOrdering;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void setColumnOrdering(int[] columnOrdering){
        this.columnOrdering=columnOrdering;
    }

    public abstract int getBaseMemoryUsage();

    public abstract void writeExternal(ObjectOutput out) throws IOException;

    public abstract void readExternal(ObjectInput in) throws IOException, ClassNotFoundException;

    public abstract int getTypeFormatId();

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof SpliceConglomerate)) return false;
        if(!super.equals(o)) return false;

        SpliceConglomerate that=(SpliceConglomerate)o;

        return containerId==that.containerId;

    }

    @Override
    public int hashCode(){
        return (int)(containerId^(containerId>>>32));
    }

    @Override
    public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void read(Row unsafeRow, int ordinal) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public int encodedKeyLength() throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void encodeIntoKey(PositionedByteRange builder, Order order) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void decodeFromKey(PositionedByteRange builder) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }
}
