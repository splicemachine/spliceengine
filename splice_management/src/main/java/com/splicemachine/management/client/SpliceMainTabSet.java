package com.splicemachine.management.client;

import com.smartgwt.client.widgets.tab.TabSet;

public class SpliceMainTabSet extends TabSet {

	public SpliceMainTabSet() {
		setHeight100();
		setWidth100();
		setHeight100();
		setWidth100();
		setTabBarControls();
		addTab(new DashboardTab());
		addTab(new SystemTab());
		addTab(new SchemaTab());
		addTab(new ActiveQueriesTab());

	}
}