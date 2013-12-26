package com.splicemachine.management.client;

import com.smartgwt.client.data.Criteria;
import com.smartgwt.client.data.DSCallback;
import com.smartgwt.client.data.DSRequest;
import com.smartgwt.client.data.DataSource;
import com.smartgwt.client.data.fields.DataSourceTextField;

public class RegionServerDataSource extends DataSource {
		public RegionServerDataSource(String id) {
			setID(id);			
			DataSourceTextField serverName = new DataSourceTextField("serverName", "Server Name");
			setFields(serverName);
		}

		@Override
		public void fetchData() {
			System.out.println("Fetch Data");
			super.fetchData();
		}

		@Override
		public void fetchData(Criteria criteria) {
			System.out.println("Fetch Data with criteria");
			super.fetchData(criteria);
		}

		@Override
		public void fetchData(Criteria criteria, DSCallback callback) {
			System.out.println("Fetch Data with criteria and callback");
			super.fetchData(criteria, callback);
		}

		@Override
		public void fetchData(Criteria criteria, DSCallback callback,
				DSRequest requestProperties) {
			System.out.println("Fetch Data with criteria and callback and request props");
			super.fetchData(criteria, callback, requestProperties);
		}
		
		
	}