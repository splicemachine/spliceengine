/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.splicemachine.db.client.net;

import java.util.Iterator;
import java.util.ServiceLoader;

public class ResourceLocalizerService {
    private static volatile ResourceLocalizer service;

    public static ResourceLocalizer getService() {
        ResourceLocalizer result = service;
        if (result == null) {
            result = loadService();
        }
        return result;
    }

    private static synchronized ResourceLocalizer loadService() {
        if (service != null)
            return service;

        ServiceLoader<ResourceLocalizer> serviceLoader = ServiceLoader.load(ResourceLocalizer.class);
        Iterator<ResourceLocalizer> it = serviceLoader.iterator();
        if (!it.hasNext()) {
            return null;
        }
        service = it.next();
        return service;
    }
}
