package com.splicemachine.constants;

import org.apache.hadoop.conf.Configuration;

public interface IHbaseConfigurationSource {

    Configuration getConfiguration();
}
