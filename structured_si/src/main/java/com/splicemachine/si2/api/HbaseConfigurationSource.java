package com.splicemachine.si2.api;

import org.apache.hadoop.conf.Configuration;

public interface HbaseConfigurationSource {

    Configuration getConfiguration();
}
