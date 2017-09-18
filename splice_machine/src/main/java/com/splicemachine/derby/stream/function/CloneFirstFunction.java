package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jleach on 8/30/17.
 */
public class CloneFirstFunction implements PairFunction<Tuple2<ExecRow,ExecRow>,ExecRow,ExecRow > {

    Set<ExecRow> set;

    public CloneFirstFunction() {
    }

    @Override
    public Tuple2<ExecRow, ExecRow> call(Tuple2<ExecRow, ExecRow> pair) throws Exception {
        if (set == null) {
            set = Collections.newSetFromMap(new LinkedHashMap<ExecRow, Boolean>(1024) {
                protected boolean removeEldestEntry(Map.Entry<ExecRow, Boolean> eldest) {
                    return size() > 1024;
                }
            });
        }
        if (!set.contains(pair._1)) { // Wish there was a better way here...
            ExecRow valueClone = pair._2.getClone();
            ExecRow keyClone = pair._1.getClone();
            set.add(keyClone);
            return Tuple2.apply(keyClone,valueClone);
        }
        return pair;
    }
}
