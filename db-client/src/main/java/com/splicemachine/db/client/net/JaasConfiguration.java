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

package com.splicemachine.db.client.net;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.HashMap;
import java.util.Map;

class JaasConfiguration extends javax.security.auth.login.Configuration {

    private static final Map<String, String> BASIC_JAAS_OPTIONS =
      new HashMap<String,String>();
    static {
      String jaasEnvVar = System.getenv("SPLICE_JAAS_DEBUG");
      if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
        BASIC_JAAS_OPTIONS.put("debug", "true");
      }
    }

    private static final Map<String,String> KEYTAB_KERBEROS_OPTIONS =
      new HashMap<String,String>();
    static {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
    }

    private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
      new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                KEYTAB_KERBEROS_OPTIONS);

    private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
            {KEYTAB_KERBEROS_LOGIN};

    private javax.security.auth.login.Configuration baseConfig;
    private final String loginContextName;
    private final boolean useTicketCache;
    private final String keytabFile;
    private final String principal;

    public JaasConfiguration(String loginContextName, String principal) {
      this(loginContextName, principal, null, true);
    }

    public JaasConfiguration(String loginContextName, String principal, String keytabFile) {
      this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.isEmpty());
    }

    private JaasConfiguration(String loginContextName, String principal,
                             String keytabFile, boolean useTicketCache) {
      try {
        this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
      } catch (SecurityException e) {
        this.baseConfig = null;
      }
      this.loginContextName = loginContextName;
      this.useTicketCache = useTicketCache;
      this.keytabFile = keytabFile;
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        if (!useTicketCache) {
          KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
          KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
        }
        KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
        KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", useTicketCache ? "true" : "false");
        return KEYTAB_KERBEROS_CONF;
      }
      if (baseConfig != null) return baseConfig.getAppConfigurationEntry(appName);
      return(null);
    }
  }
