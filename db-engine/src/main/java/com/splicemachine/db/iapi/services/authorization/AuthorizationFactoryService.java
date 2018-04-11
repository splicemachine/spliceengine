package com.splicemachine.db.iapi.services.authorization;

import java.util.Iterator;
import java.util.ServiceLoader;

public class AuthorizationFactoryService {

    public static AuthorizationFactory newAuthorizationFactory(){
        ServiceLoader<AuthorizationFactory> factoryService = ServiceLoader.load(AuthorizationFactory.class);
        Iterator<AuthorizationFactory> iter = factoryService.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No AuthorizationFactory service found!");
        AuthorizationFactory af = null;
        AuthorizationFactory currentAF = null;
        while (iter.hasNext()) {
            currentAF = (AuthorizationFactory) iter.next();
            if (af == null || af.getPriority() < currentAF.getPriority())
                af = currentAF;
        }
        return af;
    }
}
