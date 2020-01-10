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

package com.splicemachine.access.util;

import com.splicemachine.access.api.SConfiguration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by dgomezferro on 18/08/2017.
 */
public class NetworkUtils {
    public static String getHostname(SConfiguration config) {
        String hostname = config.getConfigSource().getString("hbase.regionserver.hostname",null); // Added to Support CNI Networks
        if (hostname == null) {
            hostname = config.getConfigSource().getString("hbase.master.hostname", null); // Added to Support CNI Networks
            if (hostname == null) {
                hostname = config.getConfigSource().getString("splice.olapServer.hostname", null); // Added to Support CNI Networks
                if (hostname == null) {
                    try {
                        hostname = InetAddress.getLocalHost().getHostName();
                    } catch (UnknownHostException e) {
                        hostname = "unknown";
                    }
                }
            }
        }
        return hostname;
    }
}
