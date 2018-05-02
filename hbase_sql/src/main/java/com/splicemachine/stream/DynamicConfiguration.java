package com.splicemachine.stream;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * Created by jyuan on 5/7/18.
 */
public class DynamicConfiguration
        extends javax.security.auth.login.Configuration {
    private AppConfigurationEntry[] ace;

    public DynamicConfiguration(AppConfigurationEntry[] ace) {
        this.ace = ace;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        return ace;
    }
}
