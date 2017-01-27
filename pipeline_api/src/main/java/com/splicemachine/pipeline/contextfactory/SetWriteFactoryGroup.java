/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.pipeline.context.PipelineWriteContext;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class SetWriteFactoryGroup implements WriteFactoryGroup{
    private Set<LocalWriteFactory> factories = new CopyOnWriteArraySet<>();

    @Override
    public void addFactory(LocalWriteFactory writeFactory){
        this.factories.add(writeFactory);
    }

    @Override
    public void addFactories(PipelineWriteContext context,boolean keepState,int expectedWrites) throws IOException{
        for(LocalWriteFactory lwf:factories){
            lwf.addTo(context,keepState,expectedWrites);
        }
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        factories.add(newFactory);
    }

    @Override
    public void clear(){
        factories.clear();
    }

    @Override
    public boolean isEmpty(){
        return factories.isEmpty();
    }
}
