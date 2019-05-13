/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.db.client.am;

import java.nio.charset.Charset;

public class InterruptionToken {
    private String token;
    private String uuid;
    private String address;

    public InterruptionToken(byte[] token) {
        this.token = new String(token, Charset.forName("UTF-8"));
        String[] parts = this.token.split("#");
        uuid = parts[0];
        address = parts[1];
    }

    public String getUuid() {
        return uuid;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return token;
    }
}
