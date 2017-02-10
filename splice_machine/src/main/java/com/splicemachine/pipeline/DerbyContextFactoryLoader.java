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

package com.splicemachine.pipeline;

import org.spark_project.guava.base.Optional;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Multimap;
import org.spark_project.guava.collect.Multimaps;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.PrimaryKeyConstraint;
import com.splicemachine.pipeline.constraint.UniqueConstraint;
import com.splicemachine.pipeline.contextfactory.*;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.foreignkey.FKWriteFactoryHolder;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.splicemachine.pipeline.ConglomerateDescriptors.*;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@ThreadSafe
public class DerbyContextFactoryLoader implements ContextFactoryLoader{
    private static final Logger LOG=Logger.getLogger(DerbyContextFactoryLoader.class);

    private final long conglomId;
    private final OperationStatusFactory osf;
    private final PipelineExceptionFactory pef;
    private final TransactionReadController trc;
    private final Set<ConstraintFactory> constraintFactories=new CopyOnWriteArraySet<>();
    private final FKWriteFactoryHolder fkGroup;
    private final ListWriteFactoryGroup indexFactories=new ListWriteFactoryGroup();
    private final WriteFactoryGroup ddlFactories=new SetWriteFactoryGroup();
    private final DDLWatcher.DDLListener ddlListener;

    public DerbyContextFactoryLoader(long conglomId,
                                     OperationStatusFactory osf,
                                     PipelineExceptionFactory pef,
                                     TransactionReadController trc,
                                     TxnOperationFactory txnOperationFactory){
        this.conglomId=conglomId;
        this.osf=osf;
        this.pef=pef;
        this.trc=trc;
        this.fkGroup=new FKWriteFactoryHolder(pef,txnOperationFactory);
        //TODO -sf- memory leak
        this.ddlListener=new DDLWatcher.DDLListener(){
            @Override
            public void startGlobalChange(){
            }

            @Override
            public void finishGlobalChange(){
            }

            @Override
            public void changeSuccessful(String changeId,DDLMessage.DDLChange change) throws StandardException{
            }

            @Override
            public void changeFailed(String changeId){
            }

            @Override
            public void startChange(DDLMessage.DDLChange change) throws StandardException{
                ddlChange(change);
            }
        };
    }

    @Override
    public void load(TxnView txn) throws IOException, InterruptedException{
        ContextManager currentCm=ContextService.getFactory().getCurrentContextManager();
        SpliceTransactionResourceImpl transactionResource;
        try{
            transactionResource=new SpliceTransactionResourceImpl();
            boolean prepared=false;
            try{
                prepared=transactionResource.marshallTransaction(txn);

                DataDictionary dataDictionary=transactionResource.getLcc().getDataDictionary();
                ConglomerateDescriptor conglomerateDescriptor=dataDictionary.getConglomerateDescriptor(conglomId);

                if(conglomerateDescriptor!=null){
                    dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
                    //Hbase scan
                    TableDescriptor td=dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());

                    if(td!=null){
                        startDirect(conglomId,transactionResource.getLcc(),dataDictionary,td,conglomerateDescriptor);
                    }
                }
            }catch(SQLException e){
                SpliceLogUtils.error(LOG,"Unable to acquire a database connection, aborting write, but backing"+
                        "off so that other writes can try again",e);
                throw new IndexNotSetUpException(e);
            }catch(StandardException|IOException e){
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+conglomId+", aborting",e);
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        }catch(SQLException e){
            SpliceLogUtils.error(LOG,"Unable to acquire a database connection, aborting write, but backing"+
                    "off so that other writes can try again",e);
            throw new IndexNotSetUpException(e);
        }finally{
            if(currentCm!=null)
                ContextService.getFactory().setCurrentContextManager(currentCm);
        }

    }

    @Override
    public void close(){
        DDLDriver.driver().ddlWatcher().unregisterDDLListener(ddlListener);
    }

    @Override
    public WriteFactoryGroup getForeignKeyFactories(){
        return fkGroup;
    }

    @Override
    public WriteFactoryGroup getIndexFactories(){
        return indexFactories;
    }

    @Override
    public WriteFactoryGroup getDDLFactories(){
        return ddlFactories;
    }

    @Override
    public Set<ConstraintFactory> getConstraintFactories(){
        return constraintFactories;
    }

    @Override
    public void ddlChange(DDLMessage.DDLChange ddlChange){
        DDLMessage.DDLChangeType ddlChangeType=ddlChange.getDdlChangeType();
        switch(ddlChangeType){
            case ADD_PRIMARY_KEY:
            case DROP_PRIMARY_KEY:
                // returns null if it doesn't apply to this conglomerate
                AlterTableWriteFactory writeFactory = AlterTableWriteFactory.create(ddlChange, trc,pef);
                if (writeFactory != null) {
                    ddlFactories.addFactory(writeFactory);
                }
                break;
            case ADD_FOREIGN_KEY:
                fkGroup.handleForeignKeyAdd(ddlChange,conglomId);
                break;
            case DROP_FOREIGN_KEY:
                fkGroup.handleForeignKeyDrop(ddlChange,conglomId);
                break;
            case CREATE_INDEX:
                DDLMessage.TentativeIndex tentativeIndex=ddlChange.getTentativeIndex();
                if(tentativeIndex.getTable().getConglomerate()==conglomId){
                    indexFactories.replace(IndexFactory.create(ddlChange));
                }
                break;
            case DROP_INDEX:
                if(ddlChange.getDropIndex().getBaseConglomerate()!=conglomId) break;
                TxnView txn=DDLUtils.getLazyTransaction(ddlChange.getTxnId());
                long indexConglomId=ddlChange.getDropIndex().getConglomerate();
                synchronized(indexFactories){
                    for(LocalWriteFactory factory : indexFactories.list()){
                        if(factory.getConglomerateId()==indexConglomId &&!(factory instanceof DropIndexFactory)){
                            DropIndexFactory wrappedFactory=new DropIndexFactory(txn,factory,indexConglomId);
                            indexFactories.replace(wrappedFactory);
                            return;
                        }
                    }
                    //it hasn't been added yet, so make sure that we add the index
                    indexFactories.addFactory(new DropIndexFactory(txn,null,indexConglomId));
                }
                break;
            // ignored
            default:
                break;
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    /**
     * Called once as part of context initialization.
     *
     * @param td  The table descriptor for the base table associated with the physical table this context writes to.
     * @param lcc
     */
    private boolean startDirect(long conglomId,
                                LanguageConnectionContext lcc,
                                DataDictionary dataDictionary,
                                TableDescriptor td,
                                ConglomerateDescriptor cd) throws StandardException, IOException{
        boolean isSysConglomerate=td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if(isSysConglomerate){
            SpliceLogUtils.trace(LOG,"Index management for SYS tables disabled, relying on external index management");
            return false;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // PART 1: Context configuration for PK, FK, and unique constraints using constraint descriptors.
        //
        // Here we configure the context to the extent possible using the related table's constraint
        // descriptors.  The constraint descriptors (from sys.sysconstraints) provide limited
        // information however-- see notes in part 2.
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ConstraintDescriptorList constraintDescriptors=dataDictionary.getConstraintDescriptors(td);
        for(ConstraintDescriptor cDescriptor : constraintDescriptors){
            UUID conglomerateId=cDescriptor.getConglomerateId();
            if(conglomerateId!=null && td.getConglomerateDescriptor(conglomerateId).getConglomerateNumber()!=conglomId){
                continue;
            }

            switch(cDescriptor.getConstraintType()){
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    constraintFactories.add(buildPrimaryKey(cDescriptor,osf,pef));
                    fkGroup.buildForeignKeyCheckWriteFactory((ReferencedKeyConstraintDescriptor)cDescriptor);
                    break;
                case DataDictionary.UNIQUE_CONSTRAINT:
                    buildUniqueConstraint(cDescriptor,osf,pef);
                    fkGroup.buildForeignKeyCheckWriteFactory((ReferencedKeyConstraintDescriptor)cDescriptor);
                    break;
                case DataDictionary.FOREIGNKEY_CONSTRAINT:
                    ForeignKeyConstraintDescriptor fkConstraintDescriptor=(ForeignKeyConstraintDescriptor)cDescriptor;
                    fkGroup.buildForeignKeyInterceptWriteFactory(dataDictionary,fkConstraintDescriptor);
                    break;
                default:
                    LOG.warn("Unknown Constraint on table "+conglomId+": type = "+cDescriptor.getConstraintType());
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // PART 2: Context configuration for unique/non-unique indexes using conglomerate descriptors.
        //
        // Configure indices and unique-indices for this context using the conglomerate descriptors of
        // table associated with this context.  We can only setup for non-unique indices this way,
        // because they are not classified as constraints, and thus never have an entry in the constraints
        // table. Why do we handle unique indices here then, instead of when iterating over constraints
        // above?  There seems to be a bug (or bad design?) we inherited from derby wherein unique
        // indices added by the "create index..." statement are not represented in the sys.sysconstraints
        // table.  Consequently we can only account for them by considering conglomerates, as we do here.
        // One last bit of derby weirdness is that multiple conglomerate descriptors can exist for the
        // same conglomerate number (multiple rows in sys.sysconglomerates for the same conglomerate
        // number.  This happens, for example, when a new FK constraint re-uses an existing unique or non-
        // unique index.
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        //
        // This is how we tell if the current context is being configured for a base or index table.
        //
        if(cd.isIndex()){
            // we are an index, so just map a constraint rather than an attached index
            addUniqueIndexConstraint(td,cd,osf,pef);
            // safe to clear here because we don't chain indices -- we don't send index writes to other index tables.
            indexFactories.clear();
        }
        //
        // This is a context for a base table.
        //
        else{
            Iterable<ConglomerateDescriptor> allCongloms=td.getConglomerateDescriptorList();
            // Group by conglomerate number so that we create at most one index config for each conglom number.
            Multimap<Long, ConglomerateDescriptor> numberToDescriptorMap=Multimaps.index(allCongloms,numberFunction());
            for(Long conglomerateNumber : numberToDescriptorMap.keySet()){
                // The conglomerates just for the current conglomerate number.
                Collection<ConglomerateDescriptor> currentCongloms=numberToDescriptorMap.get(conglomerateNumber);
                // Is there an index?  How about a unique index?
                Optional<ConglomerateDescriptor> indexConglom=Iterables.tryFind(currentCongloms,isIndex());
                Optional<ConglomerateDescriptor> uniqueIndexConglom=Iterables.tryFind(currentCongloms,isUniqueIndex());
                if(indexConglom.isPresent()){
                    // If this conglomerate number has a unique index conglomerate then the underlying storage (hbase
                    // table) is encoded as a unique index, so use the unique conglom.  Otherwise just use the first
                    // conglom descriptor for the current conglom number.
                    ConglomerateDescriptor srcConglomDesc=uniqueIndexConglom.isPresent()?uniqueIndexConglom.get():currentCongloms.iterator().next();
                    IndexDescriptor indexDescriptor=srcConglomDesc.getIndexDescriptor().getIndexDescriptor();

                    DDLMessage.TentativeIndex ti=ProtoUtil.createTentativeIndex(lcc,td.getBaseConglomerateDescriptor().getConglomerateNumber(),
                            indexConglom.get().getConglomerateNumber(),td,indexDescriptor);
                    IndexFactory indexFactory=IndexFactory.create(ti);
                    indexFactories.replace(indexFactory);
                }
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // PART 3: check tentative indexes
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        //TODO -sf- key this to the specific conglomerate
        DDLDriver ddlDriver=DDLDriver.driver();
        for(DDLMessage.DDLChange ddlChange : ddlDriver.ddlWatcher().getTentativeDDLs()){
            TxnView txn=DDLUtils.getLazyTransaction(ddlChange.getTxnId());
            if(txn.getEffectiveState().isFinal()){
                ddlDriver.ddlController().finishMetadataChange(ddlChange.getChangeId());
            }else{
                ddlChange(ddlChange);
            }
        }
        return true;
    }


    private void buildUniqueConstraint(ConstraintDescriptor cd,
                                       OperationStatusFactory osf,
                                       PipelineExceptionFactory pef) throws StandardException{
        String tableName=cd.getTableDescriptor().getName();
        String constraintName=cd.getConstraintName();
        ConstraintContext cc=ConstraintContext.unique(tableName,constraintName);
        constraintFactories.add(new ConstraintFactory(new UniqueConstraint(cc,osf),pef));
    }

    private void addUniqueIndexConstraint(TableDescriptor td,ConglomerateDescriptor conglomDesc,
                                          OperationStatusFactory osf,
                                          PipelineExceptionFactory pef){
        IndexDescriptor indexDescriptor=conglomDesc.getIndexDescriptor().getIndexDescriptor();
        if(indexDescriptor.isUnique()){
            String tableName=td.getName();
            String constraintName=conglomDesc.getConglomerateName();
            ConstraintContext cc=ConstraintContext.unique(tableName,constraintName);
            constraintFactories.add(new ConstraintFactory(new UniqueConstraint(cc,osf),pef));
        }
    }

    private static ConstraintFactory buildPrimaryKey(ConstraintDescriptor cDescriptor,
                                                     OperationStatusFactory osf,
                                                     PipelineExceptionFactory pef){
        String tableName=cDescriptor.getTableDescriptor().getName();
        String constraintName=cDescriptor.getConstraintName();
        ConstraintContext cc=ConstraintContext.primaryKey(tableName,constraintName);
        return new ConstraintFactory(new PrimaryKeyConstraint(cc,osf),pef);
    }

}
