package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SerializationUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
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

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class DerbyScanInformation implements ScanInformation,Externalizable {
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

    private SpliceMethod<ExecRow> resultRowAllocator;
    private SpliceMethod<ExecIndexRow> startKeyGetter;
    private SpliceMethod<ExecIndexRow> stopKeyGetter;
    private SpliceConglomerate conglomerate;
    private int colRefItem;

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
    public void initialize(SpliceOperationContext opContext) {
        this.gsps = opContext.getPreparedStatement();
        this.activation = opContext.getActivation();
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

    protected SpliceConglomerate getConglomerate() throws StandardException {
        if(conglomerate==null)
            conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(conglomId);
        return conglomerate;
    }

    @Override
    public FormatableBitSet getAccessedColumns() throws StandardException {
        if(accessedCols==null){
            if(colRefItem==-1) {
                accessedCols = null;
            }else{
                accessedCols = (FormatableBitSet)gsps.getSavedObject(colRefItem);
            }
        }
        return accessedCols;
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
        ExecIndexRow startPosition = getStartPosition();
        ExecIndexRow stopPosition = sameStartStopPosition? startPosition: getStopPosition();

        /*
         * if the stop position is the same as the start position, we are
         * right at the position where we should return values, and so we need to make sure that
         * we only return values which match an equals filter. Otherwise, we'll need
         * to scan between the start and stop keys and pull back the values which are greater than
         * or equals to the start (e.g. leave startSearchOperator alone).
         */
        if(sameStartStopPosition)
            startSearchOperator = ScanController.NA;

        Qualifier[][] qualifiers = populateQualifiers();

        getConglomerate();
        return Scans.setupScan(startPosition==null?null:startPosition.getRowArray(),
                startSearchOperator,
                stopPosition==null?null:stopPosition.getRowArray(),
                stopSearchOperator,
                qualifiers,
                conglomerate.getAscDescInfo(),
                accessedCols,
                txnId);
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
                    int columnFormat = format_ids[qualifier.getColumnId()];
                    DataValueDescriptor dvd = qualifier.getOrderable();
                    if (dvd==null)
                        continue;
                    if(dvd.getTypeFormatId()!=columnFormat){
                        //we need to convert the types to match
                        qualifier = QualifierUtils.adjustQualifier(qualifier, columnFormat,activation.getDataValueFactory());
                        qualifiers[qualPos] = qualifier;
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
        String searchOp = null;

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

}
