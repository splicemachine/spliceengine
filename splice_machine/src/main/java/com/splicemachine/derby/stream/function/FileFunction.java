package com.splicemachine.derby.stream.function;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.SpliceCsvReader;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.StringReader;

/**
 * Created by jleach on 10/8/15.
 */
    public class FileFunction extends SpliceFunction<SpliceOperation, String, LocatedRow> {
        private String characterDelimiter;
        private String columnDelimiter;
        private ExecRow execRow;
        public FileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow) {
            this.characterDelimiter = characterDelimiter;
            this.columnDelimiter = columnDelimiter;
            this.execRow = execRow;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(characterDelimiter);
            out.writeUTF(columnDelimiter);
            out.writeObject(execRow);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            characterDelimiter = in.readUTF();
            columnDelimiter = in.readUTF();
            execRow = (ExecRow) in.readObject();
        }

        @Override
        public LocatedRow call(String s) throws Exception {
                StringReader stringReader = new StringReader(s);
                SpliceCsvReader spliceCsvReader = new SpliceCsvReader(stringReader, new CsvPreference.Builder(
                        characterDelimiter.charAt(0),
                        columnDelimiter.charAt(0),
                        "\n",
                        SpliceConstants.importMaxQuotedColumnLines).useNullForEmptyColumns(false).build());
                String[] values = spliceCsvReader.readAsStringArray();
                ExecRow returnRow = execRow.getClone();
                if (values.length != returnRow.nColumns()) {
                    throw new RuntimeException("Column Missmatch with value " + s);
                } else {
                    for (int i = 1; i <= returnRow.nColumns(); i++) {
                        returnRow.getColumn(i).setValue(values[i-1]);
                    }
                    return new LocatedRow(returnRow);
                }
        }
}
