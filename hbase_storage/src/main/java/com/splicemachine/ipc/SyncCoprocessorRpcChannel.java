package com.splicemachine.ipc;

/**
 * Created by jyuan on 4/5/19.
 */
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;
import java.io.IOException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.yetus.audience.InterfaceAudience.Private;
import org.apache.yetus.audience.InterfaceAudience.Public;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Public
abstract class SyncCoprocessorRpcChannel implements CoprocessorRpcChannel {
    private static final Logger LOG = LoggerFactory.getLogger(SyncCoprocessorRpcChannel.class);

    SyncCoprocessorRpcChannel() {
    }

    @Private
    public void callMethod(MethodDescriptor method, RpcController controller, Message request, Message responsePrototype, RpcCallback<Message> callback) {
        Message response = null;

        try {
            response = this.callExecService(controller, method, request, responsePrototype);
        } catch (IOException var8) {
            LOG.warn("Call failed on IOException", var8);
            CoprocessorRpcUtils.setControllerException(controller, var8);
        }

        if(callback != null) {
            callback.run(response);
        }

    }

    @Private
    public Message callBlockingMethod(MethodDescriptor method, RpcController controller, Message request, Message responsePrototype) throws ServiceException {
        try {
            return this.callExecService(controller, method, request, responsePrototype);
        } catch (IOException var6) {
            throw new ServiceException("Error calling method " + method.getFullName(), var6);
        }
    }

    protected abstract Message callExecService(RpcController var1, MethodDescriptor var2, Message var3, Message var4) throws IOException;
}
