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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.utils.SerializationUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.utils.Pair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import static com.splicemachine.EngineDriver.isMemPlatform;
import static com.splicemachine.db.shared.common.reference.SQLState.DATA_UNEXPECTED_EXCEPTION;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

/**
 *
 *
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class MultiProbeDerbyScanInformation extends DerbyScanInformation{
    private DataValueDescriptor probeValue;
    private int inlistPosition;
    private int sortRequired;
    private String getProbeValsFuncName;
    private DataTypeDescriptor[] inlistDataTypes;
    private int inlistTypeArrayItem;
    private boolean isMemPlatform;
    Qualifier[][] qualifiers;

	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public MultiProbeDerbyScanInformation(String resultRowAllocatorMethodName,
                                          String startKeyGetterMethodName,
                                          String stopKeyGetterMethodName,
                                          String scanQualifiersField,
                                          long conglomId,
                                          int colRefItem,
                                          boolean sameStartStopPosition,
                                          int startSearchOperator,
                                          int stopSearchOperator,
                                          String getProbeValsFuncName,
                                          int sortRequired,
                                          int inlistPosition,
                                          int inlistTypeArrayItem,
                                          String tableVersion,
                                          String defaultRowMethodName,
                                          int defaultValueMapItem,
										  int numUnusedLeadingIndexFields) {
        super(resultRowAllocatorMethodName, startKeyGetterMethodName, stopKeyGetterMethodName,
                scanQualifiersField, conglomId, colRefItem, -1, sameStartStopPosition, startSearchOperator, stopSearchOperator, false,tableVersion,
				defaultRowMethodName, defaultValueMapItem, numUnusedLeadingIndexFields);
        this.getProbeValsFuncName = getProbeValsFuncName;
        this.sortRequired = sortRequired;
        this.inlistPosition = inlistPosition;
        this.inlistTypeArrayItem = inlistTypeArrayItem;
        this.isMemPlatform = isMemPlatform();
    }

    @Deprecated
    public MultiProbeDerbyScanInformation() { }

		@Override
		public ExecIndexRow getStopPosition() throws StandardException {
				ExecIndexRow stopPosition = sameStartStopPosition?super.getStartPosition():super.getStopPosition();
				if (stopPosition != null) {
					if (probeValue instanceof ListDataType) {
						ListDataType listData = (ListDataType) probeValue;
						int numVals = listData.getLength();
						for (int i = 0; i < numVals; i++) {
							stopPosition.getRowArray()[inlistPosition + i] = listData.getDVD(i);
						}
					} else
					    stopPosition.getRowArray()[inlistPosition] = probeValue;
				}
				return stopPosition;
		}

	@Override
	public ExecIndexRow getStartPosition() throws StandardException {
		ExecIndexRow startPosition = super.getStartPosition();
        if(sameStartStopPosition)
            startSearchOperator = ScanController.NA;
		if(startPosition!=null) {
			if (probeValue instanceof ListDataType) {
				ListDataType listData = (ListDataType) probeValue;
				int numVals = listData.getLength();
				for (int i = 0; i < numVals; i++) {
					startPosition.getRowArray()[inlistPosition+i] = listData.getDVD(i);
				}
			}
			else
			    startPosition.getRowArray()[inlistPosition] = probeValue;
		}
		return startPosition;
	}

	@Override
    public List<DataScan> getScans(TxnView txn, ExecRow startKeyOverride, Activation activation, int[] keyDecodingMap) throws StandardException {
        /*
         * We must build the proper scan here in pieces
         */
        BitSet colsToReturn = new BitSet();
        FormatableBitSet accessedCols = getAccessedColumns();
        if (accessedCols != null) {
            for (int i = accessedCols.anySetBit(); i >= 0; i = accessedCols.anySetBit(i)) {
                colsToReturn.set(i);
            }
        }
        List<DataScan> scans = new ArrayList<DataScan>();
        DataScan scan;

        DataValueDescriptor[] probeValues = getProbeValues();

        // Mem platform does not support the HBase MultiRangeRowFilter,
        // so we still need one scan per probe value on mem.
        if (isMemPlatform) {
			for (int i = 0; i < probeValues.length; i++) {
				probeValue = probeValues[i];
				scan = getScan(txn, null, keyDecodingMap, null, null);
				scans.add(scan);
			}
		}
		else{
			probeValue = null;
			List<Pair<byte[],byte[]>> startStopKeys =
				getStartStopKeys(txn, null, keyDecodingMap, probeValues);


			String javaCmd = System.getProperty("sun.java.command");
			boolean isOlapServer = javaCmd != null && javaCmd.startsWith("com.splicemachine.olap.OlapServerMaster");

			// On spark, threadCount is really the number of DataSets we'll produce, that
			// would need to be union'ed together.  Adjust this value to experiment with the
			// number of threads on control or number of UNION branches on Spark.
			//int threadCount = isOlapServer ? 1 : 2;
			final int threadCount = 1;

			int probesPerThread = startStopKeys.size() >= threadCount ? (startStopKeys.size() / threadCount)+1 : 3;
			int endIndex;
			for (int i=0; i < startStopKeys.size(); i+=probesPerThread) {
				probeValue = null;
				endIndex = (i+probesPerThread) <= startStopKeys.size() ? i+probesPerThread : startStopKeys.size();
				List<Pair<byte[],byte[]>> keys = startStopKeys.subList(i, endIndex);
				scan = getScan(txn, null, keyDecodingMap, null, null);
				try {
				scan.addRowkeyRangesFilter(keys);
				} catch (IOException e) {
				throw StandardException.newException(DATA_UNEXPECTED_EXCEPTION, e);
				}
				if (keys == null)
					throw StandardException.newException(LANG_INTERNAL_ERROR,
										"Multiprobe scan with no probe values.");
				scans.add(scan);
			}
		}
        return scans;
    }

    @Override
    protected Qualifier[][] populateQualifiers() throws StandardException {
		if (isMemPlatform) {
			// Mem platform would need to be synchronized if we save the
			// result into the qualifiers field.  Just return it directly...
			return populateQualifiersMain();
		}
		else {
				if (qualifiers == null) {
					return populateQualifiersMain();
				}
				else {
					return qualifiers;
				}
		}
    }


    protected Qualifier[][] populateQualifiersMain() throws StandardException {
		Qualifier[][] qualifiers;
		qualifiers = super.populateQualifiers();
		// With the MultiRowRangeFilter implementation of MultiProbeScan,
		// qualifiers are no longer built for probe values.
		if(qualifiers!=null){
			// populate the orderableCache if invariant for qualifiers, to avoid
			// setting them by multiple-threads
			for (int i = 0; i < qualifiers.length; i++) {
				if (qualifiers[i] != null) {
					for (int j = 0; j<qualifiers[i].length; j++)
						qualifiers[i][j].getOrderable();
				}
			}

		}
		return qualifiers;
	}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				SerializationUtils.writeNullableString(getProbeValsFuncName, out);
				out.writeInt(sortRequired);
				out.writeInt(inlistPosition);
                out.writeObject(probeValue);
				out.writeInt(inlistTypeArrayItem);
				out.writeBoolean(isMemPlatform);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				getProbeValsFuncName = SerializationUtils.readNullableString(in);
				sortRequired = in.readInt();
				inlistPosition = in.readInt();
                probeValue = (DataValueDescriptor)in.readObject();
				inlistTypeArrayItem = in.readInt();
				isMemPlatform = in.readBoolean();
		}

	public DataValueDescriptor[] getProbeValues() throws StandardException {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                    (getProbeValsFuncName != null),
                    "No getProbeValsFuncName found for multi-probe scan.");
        }

        // get the inlist column types
		if (inlistDataTypes == null) {
        	if (inlistTypeArrayItem != -1) {
        		inlistDataTypes = (DataTypeDescriptor[])((FormatableArrayHolder)gsps.getSavedObject(inlistTypeArrayItem)).getArray(DataTypeDescriptor.class);
			}
		}

		SpliceMethod<DataValueDescriptor[]> getProbeVals = new SpliceMethod<>(getProbeValsFuncName, activation);
		DataValueDescriptor[] probingVals = getProbeVals.invoke();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
					(probingVals != null) && (probingVals.length > 0),
					"No probe values found for multi-probe scan.");
		}

		DataValueDescriptor[] probeValues;

		if (sortRequired == RowOrdering.DONTCARE) // Already Sorted
			probeValues = probingVals;
		else {
            /* RESOLVE: For some reason sorting the probeValues array
             * directly leads to incorrect parameter value assignment when
             * executing a prepared statement multiple times.  Need to figure
             * out why (maybe related to DERBY-827?).  In the meantime, if
             * we're going to sort the values we use clones.  This is not
             * ideal, but it works for now.
             */

            // special handing for char type
			boolean[] toRemove = new boolean[probingVals.length];
			for (int position = 0; position < inlistDataTypes.length; position++) {
				DataTypeDescriptor dtd = inlistDataTypes[position];
				boolean isFixedCharType = dtd.getTypeName().equals(TypeId.CHAR_NAME);
				boolean isVarCharType = dtd.getTypeName().equals(TypeId.VARCHAR_NAME);

				if (!isFixedCharType && !isVarCharType)
					continue;

				int maxSize = dtd.getMaximumWidth();
				for (int index = 0; index < probingVals.length; index++) {
					 DataValueDescriptor dvd = probingVals[index];
					 if (dvd instanceof ListDataType) {
						 ListDataType listData = (ListDataType) dvd;
						 DataValueDescriptor dvd1 = listData.getDVD(position);
						 if (dvd1 instanceof SQLChar) {
						 	// we may prune some probe value based on the string length
						 	if (isFixedCharType && dvd1.getLength() != maxSize) {
								toRemove[index] = true;
								continue;
							} else if (isVarCharType && dvd1.getLength() > maxSize) {
								toRemove[index] = true;
								continue;
							}
							 // if column is of varchar type, we need to change the probe values from SQLChar to SQLVarchar,
							 // so that duplicate removal won't ignore the trailing spaces
							 if (isVarCharType && !(dvd1 instanceof SQLVarchar))
								 listData.setDVD(position, new SQLVarchar(dvd1.getString()));
						 }

					 } else {
					 	if (dvd instanceof SQLChar) {
							// we may prune some probe value based on the string length
							if (isFixedCharType && dvd.getLength() != maxSize) {
								toRemove[index] = true;
								continue;
							} else if (isVarCharType && dvd.getLength() > maxSize) {
								toRemove[index] = true;
								continue;
							}

							if (isVarCharType && !(dvd instanceof SQLVarchar))
								probingVals[index] = new SQLVarchar(dvd.getString());
						}
					 }
				}
			}
			// eliminate duplicates from probeValues
			HashSet<DataValueDescriptor> vset = new HashSet<>(probingVals.length);
			for (int i = 0; i < probingVals.length; i++) {
				if (!toRemove[i])
					vset.add(probingVals[i].cloneValue(false));
			}

			DataValueDescriptor[] probeValues1 = vset.toArray(new DataValueDescriptor[vset.size()]);

			if (sortRequired == RowOrdering.ASCENDING)
				Arrays.sort(probeValues1);
			else
				Arrays.sort(probeValues1, Collections.reverseOrder());
			probeValues = probeValues1;
		}
		return probeValues;
	}

	@Override
   	public void setProbeValue(DataValueDescriptor dvd) {
        probeValue = dvd;
    }
}
