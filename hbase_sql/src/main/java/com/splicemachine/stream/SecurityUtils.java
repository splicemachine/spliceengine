package com.splicemachine.stream;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jyuan on 5/9/18.
 */
public class SecurityUtils {

    private static Logger LOG = Logger.getLogger(SecurityUtils.class);

    public static UserGroupInformation loginAndReturnUGI(String loginModule,
                                                         String user,
                                                         String password) throws IOException, LoginException {

        Map<String,String> krbOptions = new HashMap();
        krbOptions.put("client", "true");
        krbOptions.put("debug", "true");
        krbOptions.put("isInitiator", "true");
        krbOptions.put("useTicketCache", "true");

        AppConfigurationEntry authEntry = new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, krbOptions);
        DynamicConfiguration dynConf = new DynamicConfiguration(new AppConfigurationEntry[]{authEntry});

        javax.security.auth.login.Configuration.setConfiguration(dynConf);

        LoginContext login = new LoginContext(loginModule, new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for(Callback c : callbacks){
                    if(c instanceof NameCallback)
                        ((NameCallback) c).setName(user);
                    if(c instanceof PasswordCallback)
                        ((PasswordCallback) c).setPassword(password.toCharArray());
                }
            }
        });
        login.login();
        UserGroupInformation ugi = UserGroupInformation.getUGIFromSubject(login.getSubject());
        LOG.error("LoginModule" + loginModule);
        LOG.error("ugi = " + ugi);
        LOG.error("UserGroupInformation.getCurrentUser() = " + UserGroupInformation.getCurrentUser());
        LOG.error("UserGroupInformation.getLoginUser() = " + UserGroupInformation.getLoginUser());
        return ugi;
    }
}
