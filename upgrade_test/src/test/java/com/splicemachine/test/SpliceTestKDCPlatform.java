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

package com.splicemachine.test;

import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.log4j.Logger;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Starts KDC server
 */
public class SpliceTestKDCPlatform {
    public static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
    public static int DEFAULT_NODE_COUNT = 1;

    private static final Logger LOG = Logger.getLogger(SpliceTestKDCPlatform.class);

    private MiniKdc kdcCluster = null;
    private LdapServer ldapServer = null;
    private Configuration conf = null;

    public SpliceTestKDCPlatform() {
    }

    public static void main(String[] args) throws Exception {
        String workDir;
        if (args != null && args.length > 0) {
            workDir = args[0];
        } else {
            throw new RuntimeException("Use main method for testing with kdc mini cluster.");
        }

        SpliceTestKDCPlatform kdcParticipant = new SpliceTestKDCPlatform();
        kdcParticipant.start(workDir);
    }

    public Configuration getConfig() {
        return conf;
    }

    public void stop() {
        if (kdcCluster != null) {
            kdcCluster.stop();
            ldapServer.stop();
        }
    }

    public void start(String path) throws Exception {
        if (kdcCluster == null) {
            Properties properties = MiniKdc.createConf();
            properties.setProperty(MiniKdc.DEBUG, "true");
            properties.setProperty(MiniKdc.KDC_PORT,"50752");

            kdcCluster = new MiniKdc(properties, new File(path));
            kdcCluster.start();

            kdcCluster.createPrincipal("splice","admin");
            List<String> principals = Arrays.asList("hbase", "hdfs", "yarn");
            List<String> extended = new ArrayList<>();
            for (String p : principals) {
                extended.add(p);
                extended.add(p + "/example.com");
            }

            kdcCluster.createPrincipal(new File(path,"splice.keytab"), extended.toArray(new String[]{}));

            File krb5conf = new File(path, "krb5.conf");
            startLdapServer(kdcCluster);
            if (kdcCluster.getKrb5conf().renameTo(krb5conf)) {
                LOG.info("LDAP server started,listening on port " + ldapServer.getPort());
                LOG.info("KDC cluster started, listening on port " + kdcCluster.getPort());
            } else {
                throw new RuntimeException("Cannot rename KDC's krb5conf to "
                        + krb5conf.getAbsolutePath());
            }

        }
    }

    public void startLdapServer(MiniKdc miniKdc) throws Exception {
        ldapServer = new LdapServer();
        Field f = MiniKdc.class.getDeclaredField("ds");
        f.setAccessible(true);
        DirectoryService ds = (DirectoryService) f.get(miniKdc);
        ldapServer.setDirectoryService(ds);
        TcpTransport tcpTransport = new TcpTransport(4016);
        ldapServer.setTransports(tcpTransport);
        LOG.info(ds.getAdminSession().getAuthenticatedPrincipal().getDn());
        ldapServer.start();
    }


}
