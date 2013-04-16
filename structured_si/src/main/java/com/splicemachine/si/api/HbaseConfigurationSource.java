package com.splicemachine.si.api;

import org.apache.hadoop.conf.Configuration;

public interface HbaseConfigurationSource {

    Configuration getConfiguration();
}
