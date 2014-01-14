package com.splicemachine.management.client;

import java.awt.TextField;
import java.util.List;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.smartgwt.client.data.DSRequest;
import com.smartgwt.client.data.DSResponse;
import com.smartgwt.client.data.Record;
import com.smartgwt.client.data.fields.DataSourceTextField;
import com.smartgwt.client.widgets.grid.ListGridRecord;

public class NTDataSource extends GwtRpcDataSource {
	private ManagementServiceAsync managementService = GWT.create(ManagementService.class);
    public NTDataSource() {        
    	this.setFields(new DataSourceTextField("serverName"));
    	
    }

    @Override
    protected void executeFetch(final String requestId, final DSRequest request,final DSResponse response) {
    	managementService.getRegionServers(new AsyncCallback<List<RegionServer>>() {
    		public void onFailure(Throwable e) {
    	        GWT.log(" connection failure ", e);  
			}

			public void onSuccess(List<RegionServer> regionServers) {
		        GWT.log(" onSuccess recieved received " + regionServers.size() + " records");       
				response.setTotalRows(regionServers.size());
		        Record returnRecords[] = new Record[regionServers.size()];
		        for (int i = 0; i < regionServers.size(); i++) {
			        GWT.log(" in here " + regionServers.get(i));       
		        	ListGridRecord r = new ListGridRecord();  
		            r.setAttribute("serverName", regionServers.get(i).getServerName());
		            returnRecords[i] = r;
		        }
		        response.setData(returnRecords);
		        processResponse(requestId,response);
			}
    		
    	});
        GWT.log(" executeFetch recieved received " + response.getTotalRows() + " records");       
    }

    @Override
    protected void executeAdd(String requestId, DSRequest request,
            DSResponse response) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void executeUpdate(String requestId, DSRequest request,
            DSResponse response) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void executeRemove(String requestId, DSRequest request,
            DSResponse response) {
        // TODO Auto-generated method stub

    }
}
