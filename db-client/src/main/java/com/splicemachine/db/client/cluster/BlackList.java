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
 *
 */

package com.splicemachine.db.client.cluster;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public abstract class BlackList<E>{
    private final Set<String> blacklistedElements = new CopyOnWriteArraySet<>();

    public void blacklist(E element){
        if(blacklistedElements.add(getElementName(element))){
            cleanupResources(element);
        }
    }

    public void whitelist(E element){
        blacklistedElements.remove(getElementName(element));
    }

    public Set<String> currentBlacklist(){
        return blacklistedElements;
    }

    protected abstract void cleanupResources(E element);

    protected String getElementName(E element){
        return element.toString();
    }

    @Override
    public String toString(){
        return blacklistedElements.toString();
    }
}
