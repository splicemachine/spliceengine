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
 *
 */

package com.splicemachine.db.impl.sql.compile;

import org.apache.log4j.Logger;

import java.util.*;

public class QueryHintingService {
    private static final Logger LOG=Logger.getLogger(QueryHintingService.class);
    private static volatile QueryHinter queryHinter;

    public static QueryHinter loadQueryHinter(){
        QueryHinter qh = queryHinter;
        if(qh==null){
            qh = loadQueryHintingSync();
        }
        return qh;
    }

    private static synchronized QueryHinter loadQueryHintingSync(){
        QueryHinter qh = queryHinter;
        if(qh==null){
            ServiceLoader<QueryHinter> load=ServiceLoader.load(QueryHinter.class);
            Iterator<QueryHinter> iter=load.iterator();
            if(!iter.hasNext()) {
                LOG.warn("No QueryHinter implementation found.");
                qh = statementNode -> {};
            } else {
                qh = queryHinter = iter.next();
            }
            if(iter.hasNext())
                throw new IllegalStateException("Only one QueryHinterService service is allowed!");
        }
        return qh;
    }
}
