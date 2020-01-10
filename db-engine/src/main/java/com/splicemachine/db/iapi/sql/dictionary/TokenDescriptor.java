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
 *
 */

package com.splicemachine.db.iapi.sql.dictionary;

import org.joda.time.DateTime;

/**
 * Created by jyuan on 6/6/17.
 */
public class TokenDescriptor extends TupleDescriptor
{
    private byte[] token;
    private String userName;
    private DateTime creationTime;
    private DateTime expireTime;
    private DateTime maxTime;

    public TokenDescriptor(byte[] token,
                           String userName,
                           DateTime creationTime,
                           DateTime expireTime,
                           DateTime maxTime)
    {
        this.token = token;
        this.userName = userName;
        this.creationTime = creationTime;
        this.expireTime = expireTime;
        this.maxTime = maxTime;
    }

    public byte[] getToken() {
        return token;
    }

    public void setToken(byte[] token) {
        this.token = token;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public DateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(DateTime creationTime) {
        this.creationTime = creationTime;
    }

    public DateTime getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(DateTime expireTime) {
        this.expireTime = expireTime;
    }

    public DateTime getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(DateTime maxTime) {
        this.maxTime = maxTime;
    }
}
