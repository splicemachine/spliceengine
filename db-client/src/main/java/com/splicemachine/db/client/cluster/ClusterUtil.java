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

import java.sql.SQLException;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * @author Scott Fines
 *         Date: 9/19/16
 */
class ClusterUtil{

    private ClusterUtil(){}

    static void logError(Logger logger, String operation,Throwable t){
       logError(logger,operation,t,Level.SEVERE);
    }

    static void logError(Logger logger, String operation,Throwable t,Level logLevel){
        String errorMessage="error during "+operation+":";
        if(t instanceof SQLException){
            SQLException se=(SQLException)t;
            errorMessage+="["+se.getSQLState()+"]";
        }
        if(t.getMessage()!=null)
            errorMessage+=t.getMessage();

        logger.log(logLevel,errorMessage,t);
    }
}
