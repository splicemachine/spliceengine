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
 */
package com.splicemachine.hbase;

import org.apache.hadoop.hbase.client.RegionInfo;

/**
 * Created by jyuan on 4/26/19.
 */
public class SpliceRegionInfo {
    private RegionInfo regionInfo;

    public SpliceRegionInfo(RegionInfo regionInfo) {
        this.regionInfo = regionInfo;
    }

    public byte[] getStartKey() {
        return regionInfo.getStartKey();
    }

    public byte[] getEndKey() {
        return regionInfo.getEndKey();
    }

    public String getEncodedName() {
        return regionInfo.getEncodedName();
    }
}
