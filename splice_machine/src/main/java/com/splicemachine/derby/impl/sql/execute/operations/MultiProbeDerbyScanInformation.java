/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.ListDataType;
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
    private DataValueDescriptor[] probeValues;
    private DataValueDescriptor probeValue;
    private int inlistPosition;
    private int sortRequired;
    private String getProbeValsFuncName;
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
                                          String tableVersion,
                                          String defaultRowMethodName,
                                          int defaultValueMapItem) {
        super(resultRowAllocatorMethodName, startKeyGetterMethodName, stopKeyGetterMethodName,
                scanQualifiersField, conglomId, colRefItem, -1, sameStartStopPosition, startSearchOperator, stopSearchOperator, false,tableVersion,
				defaultRowMethodName, defaultValueMapItem);
        this.getProbeValsFuncName = getProbeValsFuncName;
        this.sortRequired = sortRequired;
        this.inlistPosition = inlistPosition;
        this.isMemPlatform = isMemPlatform();
    }

    @Deprecated
    public MultiProbeDerbyScanInformation() { }

		@Override
		protected ExecIndexRow getStopPosition() throws StandardException {
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

        // Mem platform does not support the HBase MultiRangeRowFilter,
        // so we still need one scan per probe value on mem.
        if (isMemPlatform) {
	    for (int i = 0; i < probeValues.length; i++) {
	        probeValue = probeValues[i];
	        scan = getScan(txn, null, keyDecodingMap, null);
	        scans.add(scan);
	    }
	}
	else{
	    probeValue = null;
	    scan = getScan(txn, null, keyDecodingMap, null);
	    List<Pair<byte[],byte[]>> startStopKeys =
	        getStartStopKeys(txn, null, keyDecodingMap, probeValues);
	    try {
	        scan.setStartStopKeys(startStopKeys);
	    } catch (IOException e) {
	        throw StandardException.newException(DATA_UNEXPECTED_EXCEPTION, e);
	    }
	    if (startStopKeys == null)
	    	throw StandardException.newException(LANG_INTERNAL_ERROR,
		                        "Multiprobe scan with no probe values.");
	    scans.add(scan);
	}
        return scans;
    }

    @Override
    protected Qualifier[][] populateQualifiers() throws StandardException {
	if (isMemPlatform) {
	    synchronized (this) {
	        if (qualifiers == null) {
	            return populateQualifiersMain();
	        }
	        else {
	            return qualifiers;
	        }
	    }
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
				out.writeBoolean(probeValues!=null);
				if (probeValues != null) {
					out.writeInt(probeValues.length);
					for (DataValueDescriptor dvd : probeValues) {
						out.writeObject(dvd);
					}
				}
                		out.writeObject(probeValue);
				out.writeBoolean(isMemPlatform);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				getProbeValsFuncName = SerializationUtils.readNullableString(in);
				sortRequired = in.readInt();
				inlistPosition = in.readInt();
				if (in.readBoolean()) {
					probeValues = new DataValueDescriptor[in.readInt()];
					for (int i = 0; i < probeValues.length; i++) {
						probeValues[i] = (DataValueDescriptor) in.readObject();
					}
				}
                		probeValue = (DataValueDescriptor)in.readObject();
				isMemPlatform = in.readBoolean();

		}

	public DataValueDescriptor[] getProbeValues() throws StandardException {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                    (getProbeValsFuncName != null),
                    "No getProbeValsFuncName found for multi-probe scan.");
        }

		SpliceMethod<DataValueDescriptor[]> getProbeVals = new SpliceMethod<>(getProbeValsFuncName, activation);
		DataValueDescriptor[] probingVals = getProbeVals.invoke();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
					(probingVals != null) && (probingVals.length > 0),
					"No probe values found for multi-probe scan.");
		}


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

			// eliminate duplicates from probeValues
			HashSet<DataValueDescriptor> vset = new HashSet<DataValueDescriptor>(probingVals.length);
			for (int i = 0; i < probingVals.length; i++)
				vset.add(probingVals[i].cloneValue(false));

			DataValueDescriptor[] probeValues1 = vset.toArray(new DataValueDescriptor[vset.size()]);

			if (sortRequired == RowOrdering.ASCENDING)
				Arrays.sort(probeValues1);
			else
				Arrays.sort(probeValues1, Collections.reverseOrder());
			this.probeValues = probeValues1;
		}
		return this.probeValues;
	}

	@Override
   	public void setProbeValue(DataValueDescriptor dvd) {
        probeValue = dvd;
    }
}
