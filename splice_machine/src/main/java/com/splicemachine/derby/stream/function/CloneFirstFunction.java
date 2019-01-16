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
