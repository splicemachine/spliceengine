package com.splicemachine.si.impl;

import com.splicemachine.si.api.SIFactory;

public class SIFactoryDriver{

	public static final String SI_FACTORY_CLASS = "com.splicemachine.si.impl.SIFactoryImpl";
	public static final SIFactory siFactory;

	static {
		try {
			siFactory = (SIFactory) Class.forName(SI_FACTORY_CLASS).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static SIFactory getSIFactory() {
		return siFactory;
	}
	
}
