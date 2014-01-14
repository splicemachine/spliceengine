package com.splicemachine.management.client;

import com.smartgwt.client.data.DSRequest;
import com.smartgwt.client.data.DSResponse;
import com.smartgwt.client.data.DataSource;
import com.smartgwt.client.types.DSDataFormat;
import com.smartgwt.client.types.DSProtocol;

public abstract class GwtRpcDataSource extends DataSource {
	public GwtRpcDataSource() {
		 setDataProtocol(DSProtocol.CLIENTCUSTOM);
	     setDataFormat(DSDataFormat.CUSTOM);
	     setClientOnly(false);
	}
	
	   @Override
	    protected Object transformRequest(DSRequest request) {
	        String requestId = request.getRequestId();
	        DSResponse response = new DSResponse();
	        response.setAttribute("clientContext",
	                request.getAttributeAsObject("clientContext"));
	        // Asume success
	        response.setStatus(0);
	        switch (request.getOperationType()) {
	        case FETCH:
	            executeFetch(requestId, request, response);
	            break;
	        case ADD:
	            executeAdd(requestId, request, response);
	            break;
	        case UPDATE:
	            executeUpdate(requestId, request, response);
	            break;
	        case REMOVE:
	            executeRemove(requestId, request, response);
	            break;
	        default:
	            // Operation not implemented.
	            break;
	        }
	        return request.getData();
	    }
	   
	   protected abstract void executeFetch(String requestId, DSRequest request, DSResponse response);
	   protected abstract void executeAdd(String requestId, DSRequest request, DSResponse response);
	   protected abstract void executeUpdate(String requestId, DSRequest request, DSResponse response);
	   protected abstract void executeRemove(String requestId, DSRequest request, DSResponse response);
	
}
