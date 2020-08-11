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

package com.splicemachine.compactions;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 */
public class PurgeConfigFactoryService {
    public static PurgeConfigFactory loadPurgeConfigFactory(){
        ServiceLoader<PurgeConfigFactory> loader = ServiceLoader.load(PurgeConfigFactory.class);
        Iterator<PurgeConfigFactory> iter = loader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No PurgeConfigFactory found!");
        PurgeConfigFactory pcf = iter.next();
        if(iter.hasNext())
            throw new IllegalStateException("More than one PurgeConfigFactory is found!");
        return pcf;
    }
}
