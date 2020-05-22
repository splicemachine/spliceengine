package com.splicemachine.olap;

import java.io.IOException;
import java.util.List;

public class OlapServerNotReadyException extends IOException {
    private String queueName;
    private List<String> servers;
    private String diagnostics;

    public OlapServerNotReadyException(String queueName, List<String> servers) {
        this(queueName, servers, null);
    }

    public OlapServerNotReadyException(String queueName, List<String> servers, String diagnostics) {
        this.queueName = queueName;
        this.servers = servers;
        this.diagnostics = diagnostics;
    }

    @Override
    public String getMessage() {
        return "OlapServer for queue " + queueName + " not ready yet, available servers: " + servers + ", diagnostics:\n" + diagnostics +"\n";
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }
}
