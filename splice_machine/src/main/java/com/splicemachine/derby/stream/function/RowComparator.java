package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.io.*;
import java.util.Comparator;

/**
 * Created by jleach on 4/28/15.
 */
public class RowComparator implements Comparator<ExecRow>, Serializable, Externalizable {
    private static final long serialVersionUID = -7005014411999208729L;
    private boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending

    public RowComparator(boolean[] descColumns) {
        this.descColumns = descColumns;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(descColumns.length);
        for (int i =0; i<descColumns.length; i++)
            out.writeBoolean(descColumns[i]);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        descColumns = new boolean[in.readInt()];
        for (int i =0; i<descColumns.length; i++)
            descColumns[i] = in.readBoolean();

    }

    @Override
    public int compare(ExecRow o1, ExecRow o2) {
        DataValueDescriptor[] a1 = o1.getRowArray();
        DataValueDescriptor[] a2 = o2.getRowArray();
        for (int i = 0; i < a1.length; ++i) {
            DataValueDescriptor c1 = a1[i];
            DataValueDescriptor c2 = a2[i];
            int result;
            try {
                result = c1.compare(c2);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
            if (result != 0) {
                return descColumns[i] ? result : -result;
            }
        }
        return 0;
    }

}