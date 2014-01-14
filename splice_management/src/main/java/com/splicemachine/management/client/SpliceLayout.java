package com.splicemachine.management.client;

import com.smartgwt.client.widgets.layout.VLayout;

public class SpliceLayout extends VLayout {
	public SpliceMainTabSet spliceMainTabSet;
	public SpliceLayout() {				
			spliceMainTabSet = new SpliceMainTabSet();
			addMember(spliceMainTabSet);
			setHeight100();
			setWidth100();
	}
}
