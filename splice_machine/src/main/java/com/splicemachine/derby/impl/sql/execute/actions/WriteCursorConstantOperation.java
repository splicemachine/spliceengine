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

package com.splicemachine.derby.impl.sql.execute.actions;

import java.io.*;
import java.util.Properties;

import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.TypeMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.DataInputUtil;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.ProtobufUtils;
import com.splicemachine.db.impl.sql.CatalogMessage;
import com.splicemachine.db.impl.sql.execute.FKInfo;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 *	This abstract class describes compiled constants that are passed into
 *	Delete, Insert, and Update ResultSets.
 *
 *  This class and its sub-classes are not really implementations
 *  of ConstantAction, since they are not executed.
 *  
 *  A better name for these classes would be 'Constants'.
 *  E.g. WriteCursorConstants, DeleteConstants.
 *  
 *  Ideally one day the split will occur.
 */
public abstract	class WriteCursorConstantOperation implements ConstantAction, Formatable {

	/********************************************************
	**
	**	This class implements Formatable. But it is NOT used
 	**	across either major or minor releases.  It is only
	** 	written persistently in stored prepared statements, 
	**	not in the replication stage.  SO, IT IS OK TO CHANGE
	**	ITS read/writeExternal.
	**
	********************************************************/

	long						conglomId;
	StaticCompiledOpenConglomInfo heapSCOCI;
	IndexRowGenerator[] 		irgs;
	long[]						indexCIDS;
	StaticCompiledOpenConglomInfo[] indexSCOCIs;
	String[]					indexNames;
	boolean						deferred;
	private  Properties			targetProperties;
	UUID						targetUUID;
	int							lockMode;
	private	FKInfo[]					fkInfo;
	private TriggerInfo					triggerInfo;
	private ExecRow						emptyHeapRow;
	private FormatableBitSet baseRowReadList;
	private int[] baseRowReadMap;
	private int[] streamStorableHeapColIds;
	boolean singleRowSource;
    protected int[] pkColumns;

	/** True if this is an action of a MERGE statement */
	// todo: derby is using this to not close the activation when underMerge, but we currently don't do this.
	// todo: if we would do this, we need to check writeExternalOld, since changing this would cause
	//       SerializationTest.testGenericStorablePreparedStatement to fail
	//private boolean underMerge;

    // CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	WriteCursorConstantOperation() {}

	/**
	 *	Make the ConstantAction for a DELETE, INSERT, or UPDATE statement.
	 *
	 *  @param conglomId	Conglomerate ID of heap.
	 *	@param heapSCOCI	StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs			Index descriptors
	 *  @param indexCIDS	Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param indexNames   Names of indices on this table for error reporting.
	 *  @param deferred		True means process as a deferred update
	 *  @param targetProperties	Properties on the target table
	 *	@param targetUUID	UUID of target table
	 *	@param lockMode		The lock mode to use on the target table
	 *	@param fkInfo	Structure containing foreign key info, if any (may be null)
	 *	@param triggerInfo	Structure containing trigger info, if any (may be null)
	 *  @param emptyHeapRow	an empty heap row
	 *  @param baseRowReadMap	BaseRowReadMap[heapColId]->ReadRowColumnId. (0 based)
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param singleRowSource		Whether or not source is a single row source
	 *  @param underMerge   True if this action is under a MERGE statement
	 */
	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
	public	WriteCursorConstantOperation(
								long				conglomId,
								StaticCompiledOpenConglomInfo heapSCOCI,
                                int[] pkColumns,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								String[]			indexNames,
								boolean				deferred,
								Properties			targetProperties,
								UUID				targetUUID,
								int					lockMode,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								ExecRow				emptyHeapRow,
								FormatableBitSet				baseRowReadList,
								int[]               baseRowReadMap,
								int[]               streamStorableHeapColIds,
								boolean				singleRowSource,
								boolean				underMerge
								)
	{
		this.conglomId = conglomId;
		this.heapSCOCI = heapSCOCI;
        this.pkColumns = pkColumns;
		this.irgs = irgs;
		this.indexSCOCIs = indexSCOCIs;
		this.indexCIDS = indexCIDS;
		this.indexSCOCIs = indexSCOCIs;
		this.deferred = deferred;
		this.targetProperties = targetProperties;
		this.targetUUID = targetUUID;
		this.lockMode = lockMode;
		this.emptyHeapRow = emptyHeapRow;
		this.fkInfo = fkInfo;
		this.triggerInfo = triggerInfo;
		this.baseRowReadList = baseRowReadList;
		this.baseRowReadMap = baseRowReadMap;
		this.streamStorableHeapColIds = streamStorableHeapColIds;
		this.singleRowSource = singleRowSource;
		this.indexNames = indexNames;
		//this.underMerge = underMerge;
		if (SanityManager.DEBUG)
		{
			if (fkInfo != null)
			{
				SanityManager.ASSERT(fkInfo.length != 0, "fkinfo array has no elements, if there are no foreign keys, then pass in null");
			}
		}
	}


    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
	public int[] getPkColumns(){
        return pkColumns;
    }

	/**
	 * Basically, the same as getFKInfo but for triggers.
	 *
	 * @return	the triggers that should be fired
	 *
	 */
	public TriggerInfo getTriggerInfo() {
		return triggerInfo;
	}

    public UUID getTargetUUID() {
        return targetUUID;
    }

    /**
	 *	NOP routine. The work is done in InsertResultSet.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
	public final void executeConstantAction( Activation activation ) throws StandardException { }

	// Formatable methods
	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
    @Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		if (DataInputUtil.shouldReadOldFormat()) {
			readExternalOld(in);
		} else {
			readExternalNew(in);
		}
	}

	protected void readExternalNew( ObjectInput in ) throws IOException {
		byte[] bs = ArrayUtil.readByteArray(in);
		ExtensionRegistry extensionRegistry = ProtobufUtils.getExtensionRegistry();
		CatalogMessage.WriteCursorConstantOperation writeCursorConstantOperation =
				CatalogMessage.WriteCursorConstantOperation.parseFrom(bs, extensionRegistry);
		init(writeCursorConstantOperation);
	}

	protected void init(CatalogMessage.WriteCursorConstantOperation writeCursorConstantOperation) throws IOException{
        conglomId = writeCursorConstantOperation.getConglomId();
        if (writeCursorConstantOperation.hasHeapSCOCI()) {
        	TypeMessage.SpliceConglomerate spliceConglomerate =  writeCursorConstantOperation.getHeapSCOCI();
			heapSCOCI = SpliceConglomerate.fromProtobuf(spliceConglomerate);
		}

        int length = writeCursorConstantOperation.getIndexRowGeneratorCount();
		irgs = new IndexRowGenerator[length];
		for (int i = 0; i < length; ++i) {
			irgs[i] = ProtobufUtils.fromProtobuf(writeCursorConstantOperation.getIndexRowGenerator(i));
		}

		length = writeCursorConstantOperation.getIndexCIDSCount();
		indexCIDS = new long[length];
		for (int i = 0; i < length; ++i) {
			indexCIDS[i] = writeCursorConstantOperation.getIndexCIDS(i);
		}

		length = writeCursorConstantOperation.getIndexSCOCIsCount();
		indexSCOCIs = new StaticCompiledOpenConglomInfo[length];
		for (int i = 0; i < length; ++i) {
			indexSCOCIs[i] = SpliceConglomerate.fromProtobuf(writeCursorConstantOperation.getIndexSCOCIs(i));
		}

		deferred = writeCursorConstantOperation.getDeferred();
		if (writeCursorConstantOperation.hasTargetProperties()) {
			byte[] ba = writeCursorConstantOperation.getTargetProperties().toByteArray();
			try (ByteArrayInputStream bis = new ByteArrayInputStream(ba);
				 ObjectInputStream ois = new ObjectInputStream(bis)) {
				targetProperties = (Properties) ois.readObject();
			}
			catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		if (writeCursorConstantOperation.hasTargetUUID()) {
			targetUUID = ProtobufUtils.fromProtobuf(writeCursorConstantOperation.getTargetUUID());
		}
		lockMode = writeCursorConstantOperation.getLockMode();

		length = writeCursorConstantOperation.getFkInfoCount();
		fkInfo = new FKInfo[length];
		for (int i = 0; i < length; ++i) {
			fkInfo[i] = ProtobufUtils.fromProtobuf(writeCursorConstantOperation.getFkInfo(i));
		}

		if (writeCursorConstantOperation.hasTriggerInfo()) {
			triggerInfo = ProtobufUtils.fromProtobuf(writeCursorConstantOperation.getTriggerInfo());
		}

		if (writeCursorConstantOperation.hasBaseRowReadList()) {
			baseRowReadList = ProtobufUtils.fromProtobuf(writeCursorConstantOperation.getBaseRowReadList());
		}

		length = writeCursorConstantOperation.getBaseRowReadMapCount();
		if (length > 0) {
			baseRowReadMap = new int[length];
			for (int i = 0; i < length; ++i) {
				baseRowReadMap[i] = writeCursorConstantOperation.getBaseRowReadMap(i);
			}
		}

		length = writeCursorConstantOperation.getStreamStorableHeapColIdsCount();
		if (length > 0) {
			streamStorableHeapColIds = new int[length];
			for (int i = 0; i < length; ++i) {
				streamStorableHeapColIds[i] = writeCursorConstantOperation.getStreamStorableHeapColIds(i);
			}
		}
		singleRowSource = writeCursorConstantOperation.getSingleRowSource();

		length = writeCursorConstantOperation.getIndexNamesCount();
		if (length > 0) {
			indexNames = new String[length];
			for (int i = 0; i < length; ++i) {
				indexNames[i] = writeCursorConstantOperation.getIndexNames(i);
			}
		}

		length = writeCursorConstantOperation.getPkColumnsCount();
		if (length > 0) {
			pkColumns = new int[length];
			for (int i = 0; i < length; ++i) {
				pkColumns[i] = writeCursorConstantOperation.getPkColumns(i);
			}
		}
	}

	protected void readExternalOld( ObjectInput in ) throws IOException, ClassNotFoundException {
		conglomId = in.readLong();
		heapSCOCI = (StaticCompiledOpenConglomInfo) in.readObject();
		irgs = new IndexRowGenerator[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, irgs);

		indexCIDS = ArrayUtil.readLongArray(in);
		indexSCOCIs = new StaticCompiledOpenConglomInfo[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, indexSCOCIs);

		deferred = in.readBoolean();
		targetProperties = (Properties) in.readObject();
		targetUUID = (UUID) in.readObject();
		lockMode = in.readInt();

		fkInfo = new FKInfo[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, fkInfo);

		triggerInfo = (TriggerInfo)in.readObject();

		baseRowReadList = (FormatableBitSet)in.readObject();
		baseRowReadMap = ArrayUtil.readIntArray(in);
		streamStorableHeapColIds = ArrayUtil.readIntArray(in); 
		singleRowSource = in.readBoolean();
		indexNames = ArrayUtil.readStringArray(in);

        if(in.readBoolean()){
            pkColumns = new int[in.readInt()];
            for(int i=0;i<pkColumns.length;i++){
                pkColumns[i] = in.readInt();
            }
        }
        // underMerge = in.readBoolean();
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
    @Override
	public void writeExternal( ObjectOutput out ) throws IOException {
		if (DataInputUtil.shouldWriteOldFormat()) {
			writeExternalOld(out);
		}
		else {
			writeExternalNew(out);
		}
	}

	protected void writeExternalNew( ObjectOutput out ) throws IOException {
		CatalogMessage.WriteCursorConstantOperation writeCursorConstantOperation = toProtobufBuilder().build();
		ArrayUtil.writeByteArray(out, writeCursorConstantOperation.toByteArray());
	}

	public  CatalogMessage.WriteCursorConstantOperation.Builder toProtobufBuilder() throws IOException {
		CatalogMessage.WriteCursorConstantOperation.Builder builder =
				CatalogMessage.WriteCursorConstantOperation.newBuilder()
				.setConglomId(conglomId)
				.setDeferred(deferred)
				.setLockMode(lockMode)
				.setSingleRowSource(singleRowSource);

		if (heapSCOCI != null) {
			TypeMessage.DataValueDescriptor dvd = heapSCOCI.toProtobuf();
			TypeMessage.SpliceConglomerate spliceConglomerate =
					dvd.getExtension(TypeMessage.SpliceConglomerate.spliceConglomerate);
			builder.setHeapSCOCI(spliceConglomerate);
		}

		if (irgs != null) {
			for (int i = 0; i < irgs.length; ++i) {
				builder.addIndexRowGenerator(irgs[i].toProtobuf());
			}
		}

		if (indexCIDS != null) {
			for (int i = 0; i < indexCIDS.length; ++i) {
				builder.addIndexCIDS(indexCIDS[i]);
			}
		}
		if (indexSCOCIs != null) {
			for (int i = 0; i < indexSCOCIs.length; ++i) {
				TypeMessage.DataValueDescriptor dvd = indexSCOCIs[i].toProtobuf();
				TypeMessage.SpliceConglomerate spliceConglomerate =
						dvd.getExtension(TypeMessage.SpliceConglomerate.spliceConglomerate);
				builder.addIndexSCOCIs(spliceConglomerate);
			}
		}

		if (targetProperties != null) {
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 ObjectOutputStream os = new ObjectOutputStream(bos)) {
				os.writeObject(targetProperties);
				os.flush();
				byte[] bs = bos.toByteArray();
				builder.setTargetProperties(ByteString.copyFrom(bs));
			}
		}

		if (targetUUID != null) {
			builder.setTargetUUID(targetUUID.toProtobuf());
		}

		if (fkInfo != null) {
			for (int i = 0; i < fkInfo.length; ++i) {
				builder.addFkInfo(fkInfo[i].toProtoBuf());
			}
		}

		if (triggerInfo != null) {
			builder.setTriggerInfo(triggerInfo.toProtobuf());
		}

		if (baseRowReadList != null) {
			builder.setBaseRowReadList(baseRowReadList.toProtobuf());
		}

		if (baseRowReadMap != null) {
			for (int i = 0; i < baseRowReadMap.length; ++i) {
				builder.addBaseRowReadMap(baseRowReadMap[i]);
			}
		}

		if (streamStorableHeapColIds != null) {
			for (int i = 0; i < streamStorableHeapColIds.length; ++i) {
				builder.addStreamStorableHeapColIds(streamStorableHeapColIds[i]);
			}
		}

		if (indexNames != null) {
			for (int i = 0; i < indexNames.length; ++i) {
				if (indexNames[i] != null) {
					builder.addIndexNames(indexNames[i]);
				}
			}
		}

		if (pkColumns != null) {
			for (int i = 0; i < pkColumns.length; ++i) {
				builder.addPkColumns(pkColumns[i]);
			}
		}

		return builder;
	}

	protected void writeExternalOld( ObjectOutput out ) throws IOException {
		out.writeLong(conglomId);
		out.writeObject(heapSCOCI);
		ArrayUtil.writeArray(out, irgs);
		ArrayUtil.writeLongArray(out, indexCIDS);
		ArrayUtil.writeArray(out, indexSCOCIs);
		out.writeBoolean(deferred);
		out.writeObject(targetProperties);
		out.writeObject(targetUUID);
		out.writeInt(lockMode);
		ArrayUtil.writeArray(out, fkInfo);

		//
		//Added for Xena.
		out.writeObject(triggerInfo);

		//
		//Moved from super class for Xena.
		out.writeObject(baseRowReadList);

		//
		//Added for Xena
		ArrayUtil.writeIntArray(out,baseRowReadMap);
		ArrayUtil.writeIntArray(out,streamStorableHeapColIds);

		//Added for Buffy
		out.writeBoolean(singleRowSource);
		
		// Added for Mulan (Track Bug# 3322)
		ArrayUtil.writeArray(out, indexNames);

        out.writeBoolean(pkColumns!=null);
        if(pkColumns!=null){
            out.writeInt(pkColumns.length);
            for(int i=0;i<pkColumns.length;i++){
                out.writeInt(pkColumns[i]);
            }
        }
		//out.writeBoolean( underMerge );
	}

	// ACCESSORS

	/** Return true if this is an action of a MERGE statement */
//	public  boolean underMerge() { return underMerge; }

	/**
	 * Get the conglomerate id for the changed heap.
	 * @return the conglomerate id.
	 */
	public long getConglomerateId() { return conglomId; }

	@Override
	public long getTargetConglomId() { return getConglomerateId(); }

	public long[] getIndexCIDS() { return indexCIDS; }
		
	/**
	 *	Get emptyHeapRow
	 *
	 * @param lcc	The LanguageConnectionContext to use.
	 *
	 * @return	an empty base table row for the table being updated.
	 *
	 * @exception StandardException on error
	 */
	public ExecRow getEmptyHeapRow(LanguageConnectionContext lcc) throws StandardException
	{
		DataDictionary dd;
		TableDescriptor td;

		if (emptyHeapRow == null)
		{

			dd = lcc.getDataDictionary();
	
			td = dd.getTableDescriptor(targetUUID);
	
			emptyHeapRow = td.getEmptyExecRow();
		}

		return emptyHeapRow.getClone();
	}

	/**
	 * Get the targetProperties from the constant action.
	 *
	 * @return The targetProperties
	 */
	public Properties getTargetProperties()
	{
		return targetProperties;
	}

	/**
	 * The the value of the specified key, if it exists, from
	 * the targetProperties.
	 *
	 * @param key		The key to search for
	 *
	 * @return	The value for the specified key if it exists, otherwise null.
	 *			(Return null if targetProperties is null.)
	 */
	public String getProperty(String key)
	{
		return (targetProperties == null) ? null : targetProperties.getProperty(key);
	}

	public FormatableBitSet getBaseRowReadList() { return baseRowReadList; }

	public static WriteCursorConstantOperation fromProtobuf(
			CatalogMessage.WriteCursorConstantOperation writeCursorConstantOperation) throws IOException{

		CatalogMessage.WriteCursorConstantOperation.Type type = writeCursorConstantOperation.getType();
		switch (type) {
			case DeleteConstantOperation:
				return new DeleteConstantOperation(writeCursorConstantOperation);
			case InsertConstantOperation:
				return new InsertConstantOperation(writeCursorConstantOperation);
			case UpdateConstantOperation:
				return new UpdateConstantOperation(writeCursorConstantOperation);
			default:
				throw new RuntimeException("Unexpected type " + type);
		}
	}
}
