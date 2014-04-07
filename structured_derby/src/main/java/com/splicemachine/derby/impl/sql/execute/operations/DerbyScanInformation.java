package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SerializationUtils;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Scan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class DerbyScanInformation implements ScanInformation<ExecRow>,Externalizable {
    private static final long serialVersionUID = 1l;
    //fields marked transient as a documentation tool, so we know which fields aren't set
    private transient GenericStorablePreparedStatement gsps;
    private transient Activation activation;

    //serialized fields
    private String resultRowAllocatorMethodName;
    private String startKeyGetterMethodName;
    private String stopKeyGetterMethodName;
    private String scanQualifiersField;
    protected boolean sameStartStopPosition;
    private long conglomId;
    protected int startSearchOperator;
    protected int stopSearchOperator;


    //fields which are cached for performance
    private FormatableBitSet accessedCols;
    private FormatableBitSet accessedNonPkCols;
    private FormatableBitSet accessedPkCols;

    private SpliceMethod<ExecRow> resultRowAllocator;
    private SpliceMethod<ExecIndexRow> startKeyGetter;
    private SpliceMethod<ExecIndexRow> stopKeyGetter;
    private SpliceConglomerate conglomerate;
    private int colRefItem;
		private String tableVersion;

		@SuppressWarnings("UnusedDeclaration")
    @Deprecated
    public DerbyScanInformation() { }

    public DerbyScanInformation( String resultRowAllocatorMethodName,
                                String startKeyGetterMethodName,
                                String stopKeyGetterMethodName,
                                String scanQualifiersField,
                                long conglomId,
                                int colRefItem,
                                boolean sameStartStopPosition,
                                int startSearchOperator,
                                int stopSearchOperator){
        this.resultRowAllocatorMethodName = resultRowAllocatorMethodName;
        this.startKeyGetterMethodName = startKeyGetterMethodName;
        this.stopKeyGetterMethodName = stopKeyGetterMethodName;
        this.colRefItem = colRefItem;
        this.conglomId = conglomId;
        this.sameStartStopPosition = sameStartStopPosition;
        this.startSearchOperator = startSearchOperator;
        this.scanQualifiersField = scanQualifiersField;
        this.stopSearchOperator = stopSearchOperator;
    }

    @Override
    public void initialize(SpliceOperationContext opContext) throws StandardException {
        this.gsps = opContext.getPreparedStatement();
        this.activation = opContext.getActivation();

				DataDictionary dataDictionary = activation.getLanguageConnectionContext().getDataDictionary();
				UUID tableID = dataDictionary.getConglomerateDescriptor(conglomId).getTableID();
				TableDescriptor td = dataDictionary.getTableDescriptor(tableID);
				this.tableVersion = td.getVersion();
    }

    @Override
    public ExecRow getResultRow() throws StandardException {
        if(resultRowAllocator==null)
            resultRowAllocator = new SpliceMethod<ExecRow>(resultRowAllocatorMethodName,activation);
        return resultRowAllocator.invoke();
    }

    @Override
    public boolean isKeyed() throws StandardException {
        return  getConglomerate().getTypeFormatId()== IndexConglomerate.FORMAT_NUMBER;
    }

    public SpliceConglomerate getConglomerate() throws StandardException {
        if(conglomerate==null)
            conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(conglomId);
        return conglomerate;
    }

		@Override public String getTableVersion() throws StandardException { return tableVersion; }

		@Override
    public FormatableBitSet getAccessedColumns() throws StandardException {
        if(accessedCols==null){
            if(colRefItem==-1) {
                // accessed all columns
                accessedCols = null;
            }else{
                accessedCols = (FormatableBitSet)gsps.getSavedObject(colRefItem);
                accessedCols.grow(getConglomerate().getFormat_ids().length);
            }
        }
        return accessedCols;
    }


    @Override
    public FormatableBitSet getAccessedPkColumns() throws StandardException {
        if(accessedPkCols == null) {
            int[] columnOrdering = getColumnOrdering();
            if (columnOrdering == null)
                return null;
            FormatableBitSet cols = getAccessedColumns();
            if (cols == null) {
                int size = getConglomerate().getFormat_ids().length;
                cols = new FormatableBitSet(size);
                for (int i = 0; i < size; ++i) {
                    cols.set(i);
                }
            }
            accessedPkCols = new FormatableBitSet(cols);
            accessedPkCols.clear();
            for (int col:columnOrdering) {
                if(cols.isSet(col))
                    accessedPkCols.set(col);
            }
        }
        return accessedPkCols;
    }

    @Override
    public FormatableBitSet getAccessedNonPkColumns() throws StandardException {
        if (accessedNonPkCols == null) {
            FormatableBitSet cols = getAccessedColumns();
            if (cols == null) {
                int size = getConglomerate().getFormat_ids().length;
                cols = new FormatableBitSet(size);
                for (int i = 0; i < size; ++i) {
                    cols.set(i);
                }
            }
            accessedNonPkCols = removePkCols(cols);
        }
        return accessedNonPkCols;
    }

    private FormatableBitSet removePkCols(FormatableBitSet cols) throws StandardException {

        int[] columnOrdering = getColumnOrdering();

        if (columnOrdering == null) {
            return cols;
        } else {
            FormatableBitSet result = new FormatableBitSet(cols);
            for(int col:columnOrdering) {
                result.clear(col);
            }
            return result;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(resultRowAllocatorMethodName);
        out.writeLong(conglomId);
        out.writeBoolean(sameStartStopPosition);
        out.writeInt(startSearchOperator);
        out.writeInt(stopSearchOperator);
        out.writeInt(colRefItem);

        SerializationUtils.writeNullableString(scanQualifiersField, out);
        SerializationUtils.writeNullableString(startKeyGetterMethodName, out);
        SerializationUtils.writeNullableString(stopKeyGetterMethodName, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        resultRowAllocatorMethodName = in.readUTF();
        conglomId = in.readLong();
        sameStartStopPosition = in.readBoolean();
        startSearchOperator = in.readInt();
        stopSearchOperator = in.readInt();
        colRefItem = in.readInt();

        scanQualifiersField = SerializationUtils.readNullableString(in);
        startKeyGetterMethodName = SerializationUtils.readNullableString(in);
        stopKeyGetterMethodName = SerializationUtils.readNullableString(in);
    }

    @Override
    public Scan getScan(String txnId) throws StandardException {
        return getScan(txnId, null);
    }

    @Override
    public Scan getScan(String txnId, ExecRow startKeyOverride) throws StandardException {
        boolean sameStartStop = startKeyOverride == null && sameStartStopPosition;
        ExecIndexRow startPosition = getStartPosition();
        ExecIndexRow stopPosition = sameStartStop ? startPosition : getStopPosition();
        ExecRow overriddenStartPos = startKeyOverride != null ? startKeyOverride : startPosition;

        /*
         * if the stop position is the same as the start position, we are
         * right at the position where we should return values, and so we need to make sure that
         * we only return values which match an equals filter. Otherwise, we'll need
         * to scan between the start and stop keys and pull back the values which are greater than
         * or equals to the start (e.g. leave startSearchOperator alone).
         */
        if(sameStartStop){
            startSearchOperator = ScanController.NA;
        }

        if (startKeyOverride != null){
            startSearchOperator = ScanController.GE;
        }

        Qualifier[][] qualifiers = populateQualifiers();

        getConglomerate();
        return Scans.setupScan(
                overriddenStartPos == null ?
                            null : overriddenStartPos.getRowArray(),
                startSearchOperator,
                stopPosition == null ? null : stopPosition.getRowArray(),
                stopSearchOperator,
                qualifiers,
                conglomerate.getAscDescInfo(),
                getAccessedNonPkColumns(),
                txnId,sameStartStop,
                conglomerate.getFormat_ids(),conglomerate.getColumnOrdering(), activation.getDataValueFactory(),
								tableVersion);
    }

    @Override
    public Qualifier[][] getScanQualifiers() throws StandardException {
        return populateQualifiers();
    }

    @Override
    public long getConglomerateId() {
        return conglomId;
    }

    public String printStartPosition(int numOpens) throws StandardException {
        return printPosition(startSearchOperator, startKeyGetter, getStartPosition(),numOpens);
    }

    public String printStopPosition(int numOpens) throws StandardException {
        if (sameStartStopPosition)
            return printPosition(stopSearchOperator, startKeyGetter, getStartPosition(),numOpens);
        else
            return printPosition(stopSearchOperator, stopKeyGetter, getStopPosition(),numOpens);
    }

    protected Qualifier[][] populateQualifiers() throws StandardException {

        Qualifier[][] scanQualifiers = null;
        if (scanQualifiersField != null){
            try {
                scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
            } catch (Exception e) {
                throw StandardException.unexpectedUserException(e);
            }
        }
        //convert types of filters against column type
        if(scanQualifiers!=null){
            getConglomerate();
            int[] format_ids = conglomerate.getFormat_ids();
            for(Qualifier[] qualifiers:scanQualifiers){
                for(int qualPos=0;qualPos<qualifiers.length;qualPos++){
                    Qualifier qualifier = qualifiers[qualPos];
										qualifier.clearOrderableCache();
                    int columnFormat = format_ids[qualifier.getColumnId()];
                    DataValueDescriptor dvd = qualifier.getOrderable();
                    if (dvd==null)
                        continue;
                    if(dvd.getTypeFormatId()!=columnFormat){
                        //we need to convert the types to match
                        qualifier = QualifierUtils.adjustQualifier(qualifier, columnFormat,activation.getDataValueFactory());
                        qualifiers[qualPos] = qualifier;
                    }
                    //make sure that SQLChar qualifiers strip out \u0000 padding
                    if(dvd.getTypeFormatId()== StoredFormatIds.SQL_CHAR_ID){
                        String value = dvd.getString();
                        if(value!=null){
                            char[] valChars = value.toCharArray();
                            int finalPosition = valChars.length;
                            for(int i=valChars.length-1;i>=0;i--){
                                if(valChars[i]!='\u0000'){
                                    finalPosition=i+1;
                                    break;
                                }
                            }
                            value = value.substring(0,finalPosition);

                            dvd.setValue(value);
                        }
                    }
                }
            }

        }
        return scanQualifiers;
    }

    protected ExecIndexRow getStopPosition() throws StandardException {
        if(sameStartStopPosition)
            return null;
        if(stopKeyGetter==null &&stopKeyGetterMethodName!=null)
            stopKeyGetter = new SpliceMethod<ExecIndexRow>(stopKeyGetterMethodName,activation);

        return stopKeyGetter==null?null: stopKeyGetter.invoke();
    }

    protected ExecIndexRow getStartPosition() throws StandardException {
        if(startKeyGetter==null &&startKeyGetterMethodName!=null)
            startKeyGetter = new SpliceMethod<ExecIndexRow>(startKeyGetterMethodName,activation);

        if(startKeyGetter!=null)
            return startKeyGetter.invoke();
        return null;
    }

    /**
     * Return a start or stop positioner as a String.
     *
     * If we already generated the information, then use
     * that.  Otherwise, invoke the activation to get it.
     */
    private String printPosition(int searchOperator,
                                 SpliceMethod<ExecIndexRow> positionGetter,
                                 ExecIndexRow positioner,
                                 int numOpens)
    {
        String output = "";
        if (positionGetter == null)
            return "\t" + MessageService.getTextMessage(SQLState.LANG_NONE) + "\n";

        if (positioner == null)
        {
            if (numOpens == 0)
                return "\t" + MessageService.getTextMessage(
                        SQLState.LANG_POSITION_NOT_AVAIL) +
                        "\n";
            try {
                positioner = positionGetter.invoke();
            } catch (StandardException e) {
                return "\t" + MessageService.getTextMessage(
                        SQLState.LANG_UNEXPECTED_EXC_GETTING_POSITIONER,
                        e.toString());
            }
        }
        if (positioner == null)
            return "\t" + MessageService.getTextMessage(SQLState.LANG_NONE) + "\n";
        String searchOp;

        switch (searchOperator)
        {
            case ScanController.GE:
                searchOp = ">=";
                break;

            case ScanController.GT:
                searchOp = ">";
                break;

            default:

                // NOTE: This does not have to be internationalized because
                // this code should never be reached.
                searchOp = "unknown value (" + searchOperator + ")";
                break;
        }

        output = output + "\t" +
                MessageService.getTextMessage(
                        SQLState.LANG_POSITIONER,
                        searchOp,
                        String.valueOf(positioner.nColumns())) +
                "\n";

        output = output + "\t" +
                MessageService.getTextMessage(
                        SQLState.LANG_ORDERED_NULL_SEMANTICS) +
                "\n";
        boolean colSeen = false;
        for (int position = 0; position < positioner.nColumns(); position++)
        {
            if (positioner.areNullsOrdered(position))
            {
                output = output + position + " ";
                colSeen = true;
            }

            if (colSeen && position == positioner.nColumns() - 1)
                output = output +  "\n";
        }

        return output;
    }

	@Override
	public List<Scan> getScans(String txnId, ExecRow startKeyOverride, Activation activation, SpliceOperation top,SpliceRuntimeContext spliceRuntimeContext) throws StandardException  {
		throw new RuntimeException("getScans is not supported");
	}

    @Override
    public int[] getColumnOrdering() throws StandardException{
        return getConglomerate().getColumnOrdering();
    }

}
