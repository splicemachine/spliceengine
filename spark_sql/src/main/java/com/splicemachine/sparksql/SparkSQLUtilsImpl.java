/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.sparksql;


import com.splicemachine.utils.SparkSQLUtils;
import org.apache.spark.SparkContext;

/* Purpose:  Provide interfaces to access Spark objects or APIs
 *           which may differ depending on the Spark version.
 */
public class SparkSQLUtilsImpl implements SparkSQLUtils {
    private static volatile SparkSQLUtilsImpl INSTANCE;

    public static SparkSQLUtils getInstance(){
        SparkSQLUtilsImpl sparkSQLUtils = INSTANCE;
        if(sparkSQLUtils==null){
            synchronized(SparkSQLUtilsImpl.class){
                sparkSQLUtils = INSTANCE;
                if (sparkSQLUtils==null)
                    sparkSQLUtils = INSTANCE = new SparkSQLUtilsImpl();
            }
        }
        return sparkSQLUtils;
    }

    public void addUserJarToSparkContext(Object sc, String jarPath) {
        SparkContext sparkContext = (SparkContext)sc;
        sparkContext.addJar(jarPath);
    }
}

