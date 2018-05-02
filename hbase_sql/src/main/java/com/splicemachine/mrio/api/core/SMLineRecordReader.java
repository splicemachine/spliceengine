/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

package com.splicemachine.mrio.api.core;

import com.splicemachine.access.configuration.AuthenticationConfiguration;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.stream.SecurityUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by jyuan on 4/30/18.
 */
public class SMLineRecordReader extends LineRecordReader {

    private UserGroupInformation ugi;

    public SMLineRecordReader(byte[] recordDelimiter) {
        super(recordDelimiter);
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String authentication = conf.get(AuthenticationConfiguration.AUTHENTICATION);
        if (authentication!= null && authentication.compareToIgnoreCase(Property.AUTHENTICATION_PROVIDER_LDAP) == 0) {
            String user = conf.get("splice.user");
            String password = conf.get("splice.password");
            try {
                ugi = SecurityUtils.loginAndReturnUGI("SMLineRecordReader", user, password);
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                    public Void run() throws Exception {
                        init(genericSplit, context);
                        return null;
                    }
                });
            } catch (Exception lex) {
                throw new IOException(lex);
            }
        }
        else {
            init(genericSplit, context);
        }
    }

    private void init(InputSplit genericSplit, TaskAttemptContext context) throws IOException{
        super.initialize(genericSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        try {
            if (ugi != null) {
                return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
                    public Boolean run() throws Exception {
                        return getNextKeyValue();
                    }
                });
            } else {
                return getNextKeyValue();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private boolean getNextKeyValue() throws IOException {
        return super.nextKeyValue();
    }
}
