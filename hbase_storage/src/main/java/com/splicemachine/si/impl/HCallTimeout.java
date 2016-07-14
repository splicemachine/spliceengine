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

import com.splicemachine.access.api.CallTimeoutException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/8/16
 */
public class HCallTimeout extends IOException implements CallTimeoutException{
    public HCallTimeout(){
    }

    public HCallTimeout(String message){
        super(message);
    }

    public HCallTimeout(String message,Throwable cause){
        super(message,cause);
    }

    public HCallTimeout(Throwable cause){
        super(cause);
    }
}
