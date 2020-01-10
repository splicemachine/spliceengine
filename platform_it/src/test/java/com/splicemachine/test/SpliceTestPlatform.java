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

package com.splicemachine.test;

import com.splicemachine.hbase.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

/**
 * Start MiniHBaseCluster for use by ITs.
 */
public class SpliceTestPlatform {

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            SpliceTestPlatformUsage.usage("Unknown argument(s)", null);
        }
        try {

            String hbaseRootDirUri = args[0];
            Integer masterPort = Integer.valueOf(args[1]);
            Integer masterInfoPort = Integer.valueOf(args[2]);
            Integer regionServerPort = Integer.valueOf(args[3]);
            Integer regionServerInfoPort = Integer.valueOf(args[4]);
            Integer derbyPort = Integer.valueOf(args[5]);
            boolean failTasksRandomly = Boolean.valueOf(args[6]);
            String olapLog4jConfig = args[7];
            boolean secure = Boolean.parseBoolean(args[8]);

            Configuration config = SpliceTestPlatformConfig.create(
                    hbaseRootDirUri,
                    masterPort,
                    masterInfoPort,
                    regionServerPort,
                    regionServerInfoPort,
                    derbyPort,
                    failTasksRandomly,
                    olapLog4jConfig,
                    secure);

            // clean-up zookeeper
            try {
                ZkUtils.delete("/hbase/master");
            } catch (KeeperException.NoNodeException ex) {
                // ignore
            }
            try {
                ZkUtils.recursiveDelete("/hbase/rs");
            } catch (KeeperException.NoNodeException | IOException ex) {
                // ignore
            }

            String keytab = hbaseRootDirUri+"/splice.keytab";
            UserGroupInformation ugi;
            if (secure) {
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/example.com@EXAMPLE.COM", keytab);
                UserGroupInformation.setLoginUser(ugi);
            } else {
                ugi = UserGroupInformation.getCurrentUser();
            }
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    MiniHBaseCluster miniHBaseCluster = new MiniHBaseCluster(config, 1, 1);
                    new SpliceTestPlatformShutdownThread(miniHBaseCluster);

                    Configuration hbaseConf = miniHBaseCluster.getConfiguration();
                    hbaseConf.writeXml(bytesOut);
                    bytesOut.close();

                    return null;
                }

            });

            String hbaseSiteFile = hbaseRootDirUri+"/hbase-site.xml";
            File file = new File(new URL(hbaseSiteFile).getPath());
            file.createNewFile();
            //write the bytes to the file in the classpath
            OutputStream os = new FileOutputStream(file);
            os.write(bytesOut.toByteArray());
            os.close();

        } catch (NumberFormatException e) {
            SpliceTestPlatformUsage.usage("Bad port specified", e);
        }
    }

}
