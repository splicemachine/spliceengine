package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;

/**
 * This class records the window definition (partition, orderby and frame)
 * for a window function.
 *
 * @author Jeff Cunningham
 *         Date: 7/8/14
 */
public class DerbyWindowContext implements WindowContext {
    private Activation activation;
    private int partitionItemIdx;
    private int orderingItemIdx;
    private int frameDefnIdx;
    private int[] partitionColumns;
    private int[] sortColumns;
    private boolean[] sortOrders;
    private int[] keyColumns;
    private boolean[] keyOrders;
    FrameDefinition frameDefinition;

    public DerbyWindowContext() {
    }

    public DerbyWindowContext(int partitionItemIdx, int orderingItemIdx, int frameDefnIdx) {
        this.partitionItemIdx = partitionItemIdx;
        this.orderingItemIdx = orderingItemIdx;
        this.frameDefnIdx = frameDefnIdx;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        this.activation = context.getActivation();

        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        ColumnOrdering[] partition = (ColumnOrdering[])
            ((FormatableArrayHolder) (statement.getSavedObject(partitionItemIdx))).getArray(ColumnOrdering.class);

        ColumnOrdering[] orderings = (ColumnOrdering[])
            ((FormatableArrayHolder) (statement.getSavedObject(orderingItemIdx))).getArray(ColumnOrdering.class);

        frameDefinition = FrameDefinition.create((FormatableHashtable) statement.getSavedObject(frameDefnIdx));

        keyColumns = new int[partition.length + orderings.length];
        keyOrders = new boolean[partition.length + orderings.length];

        partitionColumns = new int[partition.length];
        sortColumns = new int[orderings.length];

        int pos=0;
        for(ColumnOrdering partCol:partition){
            partitionColumns[pos] = partCol.getColumnId();
            keyColumns[pos] = partCol.getColumnId();
            keyOrders[pos] = partCol.getIsAscending();
            pos++;
        }
        pos = 0;
        for(ColumnOrdering order:orderings){
            sortColumns[pos] = order.getColumnId();
            keyColumns[partition.length + pos] = order.getColumnId();
            keyOrders[partition.length + pos] = order.getIsAscending();
            pos++;
        }
    }

    private boolean keysContain(int[] keyColumns, int inputColNum) {
        for(int keyColumn:keyColumns){
            if(keyColumn==inputColNum)
                return true;
        }
        return false;
    }

    @Override
    public int[] getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public int[] getSortColumns() {
        return sortColumns;
    }

    @Override
    public boolean[] getSortOrders() {
        return sortOrders;
    }


    @Override
    public void addWarning(String warningState) throws StandardException {
        activation.addWarning(SQLWarningFactory.newSQLWarning(warningState));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partitionItemIdx);
        out.writeInt(orderingItemIdx);
        out.writeInt(frameDefnIdx);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partitionItemIdx = in.readInt();
        orderingItemIdx = in.readInt();
        frameDefnIdx = in.readInt();
    }

    @Override
    public int[] getKeyColumns() {
        return keyColumns;
    }

    @Override
    public boolean[] getKeyOrders() {
        return keyOrders;
    }

    @Override
    public FrameDefinition getFrameDefinition() {
        return frameDefinition;
    }

}
