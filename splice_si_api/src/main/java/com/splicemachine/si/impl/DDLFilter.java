/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.Txn;
import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DDLFilter implements Comparable<DDLFilter> {
    private final Txn txn;
    private Cache<Long,Boolean> visibilityMap;

		public DDLFilter(Txn txn) {
            assert txn!=null:"txn passed in cannot be null";
            this.txn = txn;
            visibilityMap = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();
		}

		public boolean isVisibleBy(final Txn canSeeTxn) throws IOException {
            return true;
		}


    public Txn getTransaction() {
				return txn;
		}

    @Override
    public boolean equals(Object o){
        return this==o || o instanceof DDLFilter && compareTo((DDLFilter)o)==0;
    }

    @Override
    public int hashCode(){
        return txn.hashCode();
    }

    @Override
    public int compareTo(DDLFilter o) {
        return this.txn.compareTo(o.txn);
    }
}
