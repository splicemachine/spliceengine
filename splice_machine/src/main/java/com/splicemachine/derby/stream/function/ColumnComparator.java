package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.util.Comparator;

/**
 * Created by jleach on 4/28/15.
 */
public class ColumnComparator implements Comparator<ExecRow>, Serializable, Externalizable {
    private static final long serialVersionUID = -7005014411999208729L;
    private int[] columns;
    private boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
    private boolean nullsOrderedLow;

    public ColumnComparator() {

    }
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ColumnComparator(int[] columns, boolean[] descColumns, boolean nullsOrderedLow) {
        this.columns = columns;
        this.descColumns = descColumns;
        this.nullsOrderedLow = nullsOrderedLow;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(columns.length);
        for (int c : columns)
            out.writeInt(c);
        out.writeInt(descColumns.length);
        for (int i =0; i<descColumns.length; i++)
            out.writeBoolean(descColumns[i]);
        out.writeBoolean(nullsOrderedLow);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        columns = new int[in.readInt()];
        for (int i = 0; i < columns.length; i++)
            columns[i] = in.readInt();
        descColumns = new boolean[in.readInt()];
        for (int i = 0; i < descColumns.length; i++)
            descColumns[i] = in.readBoolean();
         nullsOrderedLow = in.readBoolean();
    }

    @Override
    @SuppressFBWarnings(value = "RV_NEGATING_RESULT_OF_COMPARETO",justification = "Intentional")
    public int compare(ExecRow o1, ExecRow o2) {
        DataValueDescriptor[] a1 = o1.getRowArray();
        DataValueDescriptor[] a2 = o2.getRowArray();
        for (int i = 0; i < columns.length; ++i) {
            DataValueDescriptor c1 = a1[columns[i]];
            DataValueDescriptor c2 = a2[columns[i]];
            int result;
            try {
                result = c1.compare(c2,nullsOrderedLow);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
            if (result != 0) {
                if (nullsOrderedLow && (c1.isNull() || c2.isNull()))
                    return result; // nulls go first independently of descColumns
                return (descColumns==null ||descColumns[i]) ? result : -result;
            }
        }
        return 0;
    }

}