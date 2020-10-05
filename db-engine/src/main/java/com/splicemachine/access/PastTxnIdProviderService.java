/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.access;

import java.util.Iterator;
import java.util.ServiceLoader;

public class PastTxnIdProviderService {
    private static volatile PastTxnIdProvider txnIdProvider;

    public static PastTxnIdProvider loadPropertyManager(){
        PastTxnIdProvider pm = txnIdProvider;
        if(pm==null){
            pm = loadPropertyManagerSync();
        }
        return pm;
    }

    private static synchronized PastTxnIdProvider loadPropertyManagerSync(){
        PastTxnIdProvider pm = txnIdProvider;
        if(pm==null){
            ServiceLoader<PastTxnIdProvider> load=ServiceLoader.load(PastTxnIdProvider.class);
            Iterator<PastTxnIdProvider> iter=load.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No PastTxnIdProviderService service found!");
            pm = txnIdProvider = iter.next();
            if(iter.hasNext())
                throw new IllegalStateException("Only one PastTxnIdProviderService service is allowed!");
        }
        return pm;
    }
}
