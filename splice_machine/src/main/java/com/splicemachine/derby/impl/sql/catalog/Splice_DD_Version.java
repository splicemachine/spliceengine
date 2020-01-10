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

package com.splicemachine.derby.impl.sql.catalog;

import com.esotericsoftware.kryo.KryoException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.catalog.DD_Version;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.log4j.Logger;
/**
 * Created by jyuan on 10/13/14.
 */
public class Splice_DD_Version extends DD_Version {
    private static final Logger LOG = Logger.getLogger(Splice_DD_Version.class);
    private int patchVersionNumber;
    private int sprintVersionNumber;

    public Splice_DD_Version() {

    }

    public Splice_DD_Version (DataDictionaryImpl bootingDictionary, int major, int minor, int patch) {
        this(bootingDictionary, major, minor, patch, 0);
    }
    public Splice_DD_Version (DataDictionaryImpl bootingDictionary, int major, int minor, int patch, int sprint) {
        this.bootingDictionary = bootingDictionary;
        this.majorVersionNumber = major;
        this.minorVersionNumber = minor;
        this.patchVersionNumber = patch;
        this.sprintVersionNumber = sprint;
    }

    public Splice_DD_Version (DataDictionaryImpl bootingDictionary, String versionString) throws StandardException {
    	if (versionString.matches("\\d(\\.\\d)?(\\.\\d)?(\\.\\d+)?")) {
            this.majorVersionNumber = 0;
            this.minorVersionNumber = 0;
            this.patchVersionNumber = 0;
            this.sprintVersionNumber = 0;
    		String[] versionArray = versionString.split("\\.");
    		if (versionArray.length > 0) {
    			this.majorVersionNumber = Integer.parseInt(versionArray[0]);
    			if (versionArray.length > 1) {
    				this.minorVersionNumber = Integer.parseInt(versionArray[1]);
    				if (versionArray.length > 2) {
    					this.patchVersionNumber = Integer.parseInt(versionArray[2]);
                        if (versionArray.length > 3) {
                            this.sprintVersionNumber = Integer.parseInt(versionArray[3]);
                        }
    				}
    			}
    		}
    		this.bootingDictionary = bootingDictionary;
    	} else {
    		// Bad version string...
    		throw StandardException.newException(String.format("Version string does not match \"major.minor.patch\" pattern: %s", versionString));
    	}
    }

    public int getMajorVersionNumber() {
        return majorVersionNumber;
    }

    public int getMinorVersionNumber() {
        return minorVersionNumber;
    }

    public int getPatchVersionNumber() {
        return patchVersionNumber;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(majorVersionNumber).append(".").append(minorVersionNumber).append(".").append(patchVersionNumber);
        if (sprintVersionNumber > 0) {
            sb.append('.').append(sprintVersionNumber);
        }
        return sb.toString();
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException {
        super.readExternal(in);
        patchVersionNumber = in.readInt();
        try {
            sprintVersionNumber = in.readInt();
        } catch (KryoException ke) {
            // we didn't have a sprint version number, leave it at 0
            LOG.warn("Couldn't read sprintVersionNumber field, must be and old format");
        }
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        super.writeExternal(out);
        out.writeInt(patchVersionNumber);
        out.writeInt(sprintVersionNumber);
    }

    public long toLong() {
        return majorVersionNumber * 10000000000l + minorVersionNumber * 10000000l + patchVersionNumber * 10000l + sprintVersionNumber;
    }

}
