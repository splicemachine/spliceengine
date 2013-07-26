package com.splicemachine.derby.utils;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.util.List;

/**
 * Indicates a bounds on a Scan
 * @author Scott Fines
 * Created on: 3/6/13
 */
public enum QualifierBounds {
    LESS_THAN {
        @Override
        public QualifierBounds compare(QualifierBounds other){
            switch (other) {
                case EQUALS:
                    return other;
                case GREATER_THAN:
                    return LESS_THAN_GREATER_THAN;
                case GREATER_THAN_EQUALS:
                    return LESS_THAN_GREATER_EQUALS;
                default:
                    return this;
            }
        }

        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            shouldContinue[0] = false;
            if(shouldContinue[1])stops.add(DerbyBytesUtil.generateBytes(bestStop));
        }
    },
    LESS_THAN_EQUALS {
        @Override
        public QualifierBounds compare(QualifierBounds other){
            switch (other) {
                case LESS_THAN:
                    return other;
                case EQUALS:
                    return other;
                case GREATER_THAN:
                    return LESS_EQUALS_GREATER_THAN;
                case GREATER_THAN_EQUALS:
                    return LESS_EQUALS_GREATER_EQUALS;
                default:
                    return this;
            }
        }

        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            shouldContinue[0] = false;
            if(shouldContinue[1]) stops.add(DerbyBytesUtil.generateBytes(bestStop));
        }
    },
    EQUALS{
        //equals always wins
        @Override
        public QualifierBounds compare(QualifierBounds other){ return this;  }

        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            byte[] startBytes = DerbyBytesUtil.generateBytes(bestStart);
            if(shouldContinue[0]) starts.add(startBytes);
            if(shouldContinue[1]) stops.add(startBytes);
        }
    },
    GREATER_THAN{
        @Override
        public QualifierBounds compare(QualifierBounds other){
            switch (other) {
                case LESS_THAN:
                    return LESS_THAN_GREATER_THAN;
                case LESS_THAN_EQUALS:
                    return LESS_EQUALS_GREATER_THAN;
                case EQUALS:
                    return other;
                default:
                    return this;
            }
        }

        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            shouldContinue[1] = false;
            if(shouldContinue[0]) starts.add(getIncrementedBytes(bestStart));
        }
    },
    GREATER_THAN_EQUALS{
        @Override
        public QualifierBounds compare(QualifierBounds other){
            switch (other) {
                case LESS_THAN:
                    return LESS_THAN_GREATER_EQUALS;
                case LESS_THAN_EQUALS:
                    return LESS_EQUALS_GREATER_EQUALS;
                case EQUALS:
                case GREATER_THAN:
                    return other;
                default:
                    return this;
            }
        }

        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            shouldContinue[1] = false;
            if(shouldContinue[0]) starts.add(DerbyBytesUtil.generateBytes(bestStart));
        }
    },
    NOT_EQUALS{
        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            shouldContinue[0] = false;
            shouldContinue[1] = false;
        }

        //not equals never wins
        public QualifierBounds compare(QualifierBounds other){
            return other;
        }
    },
    LESS_THAN_GREATER_THAN{
        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            if(shouldContinue[0]) starts.add(DerbyBytesUtil.generateBytes(bestStart));
            if(shouldContinue[1]) stops.add(DerbyBytesUtil.generateBytes(bestStop));
        }
    },
    LESS_THAN_GREATER_EQUALS{
        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            if(shouldContinue[0]) starts.add(DerbyBytesUtil.generateBytes(bestStart));
            if(shouldContinue[1]) stops.add(DerbyBytesUtil.generateBytes(bestStop));
        }
    },
    LESS_EQUALS_GREATER_THAN{
        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            if(shouldContinue[0])starts.add(getIncrementedBytes(bestStart));
            if(shouldContinue[1])stops.add(DerbyBytesUtil.generateBytes(bestStop));
        }
    },
    LESS_EQUALS_GREATER_EQUALS{
        @Override
        protected void setBounds(DataValueDescriptor bestStart,
                                 DataValueDescriptor bestStop,
                                 List<byte[]> starts,
                                 List<byte[]> stops,
                                 boolean[] shouldContinue) throws StandardException, IOException {
            if(shouldContinue[0])starts.add(DerbyBytesUtil.generateBytes(bestStart));
            if(shouldContinue[1])stops.add(DerbyBytesUtil.generateBytes(bestStop));
        }
    };


    QualifierBounds() { }

    protected QualifierBounds compare(QualifierBounds other){ return this;}

    public final void process(List<Qualifier> qualifiers,
                        List<byte[]> starts,
                        List<byte[]> stops,
                        boolean[] shouldContinue) throws StandardException, IOException {
        DataValueDescriptor bestStart = null;
        DataValueDescriptor bestStop = null;
        for(Qualifier qualifier:qualifiers){
            DataValueDescriptor other = qualifier.getOrderable();
            if(bestStart==null||bestStart.compare(other)>0)
                bestStart = other;
            if(bestStop==null||bestStop.compare(other)<0)
                bestStop = other;
        }

        setBounds(bestStart,bestStop,starts,stops,shouldContinue);
    }

    private static byte[] getIncrementedBytes(DataValueDescriptor bestStop) throws IOException, StandardException {
        byte[] val = DerbyBytesUtil.generateBytes(bestStop);
        BytesUtil.incrementAtIndex(val,val.length-1);
        return val;
    }

    protected void setBounds(DataValueDescriptor bestStart,
                             DataValueDescriptor bestStop,
                             List<byte[]>starts,
                             List<byte[]> stops, boolean[] shouldContinue) throws StandardException,IOException{
        throw new AbstractMethodError();
    }

    public static QualifierBounds getOperator(List<Qualifier> qualifiers){
        QualifierBounds currentQualifierBounds = QualifierBounds.NOT_EQUALS;
        if(qualifiers==null) return currentQualifierBounds; //return NOT_EQUALS since it's a noop
        for(Qualifier qualifier:qualifiers){
            currentQualifierBounds = currentQualifierBounds.compare(getOperator(qualifier));
        }
        return currentQualifierBounds;
    }

    private static QualifierBounds getOperator(Qualifier qualifier) {
        if(qualifier.negateCompareResult()){
            switch(qualifier.getOperator()){
                case DataValueDescriptor.ORDER_OP_EQUALS: return NOT_EQUALS;
                case DataValueDescriptor.ORDER_OP_LESSTHAN: return GREATER_THAN_EQUALS;
                case DataValueDescriptor.ORDER_OP_GREATERTHAN: return LESS_THAN_EQUALS;
                case DataValueDescriptor.ORDER_OP_LESSOREQUALS: return GREATER_THAN;
                case DataValueDescriptor.ORDER_OP_GREATEROREQUALS: return LESS_THAN;
                default:
                    return NOT_EQUALS; //when in doubt, not equals is what we want
            }
        }else{
            switch(qualifier.getOperator()){
                case DataValueDescriptor.ORDER_OP_EQUALS: return EQUALS;
                case DataValueDescriptor.ORDER_OP_LESSTHAN: return LESS_THAN;
                case DataValueDescriptor.ORDER_OP_LESSOREQUALS: return LESS_THAN_EQUALS;
                case DataValueDescriptor.ORDER_OP_GREATERTHAN: return GREATER_THAN;
                case DataValueDescriptor.ORDER_OP_GREATEROREQUALS: return GREATER_THAN_EQUALS;
                default:
                    return NOT_EQUALS; //when in doubt, not equals is what we want
            }
        }
    }

    public static boolean isLessThanOperator(QualifierBounds qualifierBounds) {
        return qualifierBounds==LESS_THAN||
                qualifierBounds==LESS_THAN_GREATER_EQUALS
                || qualifierBounds == LESS_THAN_GREATER_THAN;
    }
}
