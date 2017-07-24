package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.types.SQLVarchar;
import org.apache.spark.api.java.function.Function;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.Row;

/**
 *
 * Allows a map to convert from RDD<ExecRow> to RDD<Row>
 *
 */
public class LocatedRowToRowAvroFunction implements Function<ExecRow, Row> {
    public LocatedRowToRowAvroFunction() {
        super();
    }
    @Override
    public Row call(ExecRow locatedRow) throws Exception {
        for(int i=0; i < locatedRow.size(); i++){
            if (locatedRow.getColumn(i + 1).getTypeName().equals("DATE")){
                if (locatedRow.getColumn(i + 1).isNull()){
                    locatedRow.setColumn(i + 1, new SQLVarchar());
                } else {
                    locatedRow.setColumn(i + 1, new SQLVarchar(locatedRow.getColumn(i + 1).getString().toCharArray()));
                }
            }
        }
        return (Row)locatedRow;
    }
}