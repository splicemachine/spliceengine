package com.splicemachine.management.client;

import com.google.gwt.user.client.Timer;
import com.smartgwt.client.data.DataSource;
import com.smartgwt.client.widgets.grid.ListGrid;
import com.smartgwt.client.widgets.layout.VLayout;
import com.smartgwt.client.widgets.tab.Tab;

public class DashboardTab extends Tab {
	VLayout layout = new VLayout();
	ListGrid companyGrid;
	public DashboardTab() {
		super();
		this.setTitle("Dashboard");
		Timer timer = new Timer(){
			@Override
			public void run() {
				companyGrid.fetchData();
			}};
		timer.schedule(5000);
		DataSource dataSource = new NTDataSource();
		dataSource.setClientOnly(true);
        companyGrid = new ListGrid();
        companyGrid.setHeight(300);
        companyGrid.setWidth(500);
        companyGrid.setTitle("SmartGWT grid");
        companyGrid.setDataSource(dataSource);
        companyGrid.setAutoFetchData(true);
        layout.addMember(companyGrid);
        this.setPane(layout);	
	}
	
}
