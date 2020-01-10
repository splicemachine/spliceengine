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

package com.splicemachine.pipeline.client;

import java.io.IOException;

/**
 * Exception to indicate that a write was failed.
 *
 * @author Scott Fines
 *         Date: 4/19/16
 */
public class WriteFailedException extends IOException{

    private String tableName;
    private int attemptCount;


    public WriteFailedException(String tableName,int attemptCount){
        this.tableName = tableName;
        this.attemptCount=attemptCount;
    }

    public WriteFailedException(){ }

    public WriteFailedException(String message){
        super(message);
    }

    public WriteFailedException(String message,Throwable cause){
        super(message,cause);
    }

    public WriteFailedException(Throwable cause){
        super(cause);
    }

    public String getDestinationTableName(){
        return tableName;
    }

    public int getNumAttempts(){
        return attemptCount;
    }
}
