package com.splicemachine.olap;

import java.io.IOException;
import java.util.List;

public class OlapServerNotReadyException extends IOException {
    private String queueName;
    private List<String> servers;

    public OlapServerNotReadyException(String queueName, List<String> servers) {
        this.queueName = queueName;
        this.servers = servers;
    }

    @Override
    public String getMessage() {
        return "OlapServer for queue " + queueName + " not ready yet, available servers: " + servers;
    }
}
