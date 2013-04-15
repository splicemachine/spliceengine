package com.splicemachine.si2.si.api;

import org.apache.hadoop.conf.Configuration;

public interface HbaseConfigurationSource {

    Configuration getConfiguration();
}
