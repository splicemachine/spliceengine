package com.splicemachine.si.impl;

import com.splicemachine.si.api.SIFactory;

public class SIFactoryDriver {
	public static String SI_FACTORY_CLASS = "com.splicemachine.si.impl.SIFactoryImpl";
	public static SIFactory siFactory;
	
	static {
		try {
			siFactory = (SIFactory) Class.forName(SI_FACTORY_CLASS).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public SIFactory getSIFactory() {
		return siFactory;
	}
	
}
