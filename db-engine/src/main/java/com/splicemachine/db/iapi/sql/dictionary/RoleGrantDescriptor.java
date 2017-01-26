/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * This class is used by rows in the SYS.SYSROLES system table.
 *
 * An instance contains information for exactly: One &lt;role
 * definition&gt;, cf. ISO/IEC 9075-2:2003 section 12.4
 * <bold>or</bold> one &lt;grant role statement&gt;, section 12.5.
 *
 * A role definition is also modeled as a role grant (hence the class
 * name), but with the special grantor "_SYSTEM", and with a grantee
 * of the definer, in Derby this is always the current user. For a
 * role definition, the WITH ADMIN flag is also set. The information
 * contained in the isDef flag is usually redundant, but was added as
 * a precaution against a real user named _SYSTEM, for example when
 * upgrading an older database that did not forbid this.
 */
public class RoleGrantDescriptor extends TupleDescriptor
    implements Provider
{
    private final UUID uuid;
    private final String roleName;
    private final String grantee;
    private final String grantor;
    private boolean withAdminOption;
    private final boolean isDef; // if true, represents a role
                                 // definition, else a grant

    /**
     * Constructor
     *
     * @param dd data dictionary
     * @param uuid  unique identification in time and space of this role
     *              descriptor
     * @param roleName
     * @param grantee
     * @param grantor
     * @param withAdminOption
     * @param isDef
     *
     */
    public RoleGrantDescriptor(DataDictionary dd,
                               UUID uuid,
                               String roleName,
                               String grantee,
                               String grantor,
                               boolean withAdminOption,
                               boolean isDef) {
        super(dd);
        this.uuid = uuid;
        this.roleName = roleName;
        this.grantee = grantee;
        this.grantor = grantor;
        this.withAdminOption = withAdminOption;
        this.isDef = isDef;
    }

    public UUID getUUID() {
        return uuid;
    }

    public String getGrantee() {
        return grantee;
    }

    public String getGrantor() {
        return grantor;
    }

    public boolean isDef() {
        return isDef;
    }

    public String getRoleName() {
        return roleName;
    }

    public boolean isWithAdminOption() {
        return withAdminOption;
    }

    public void setWithAdminOption(boolean b) {
        withAdminOption = b;
    }

    public String toString() {
        if (SanityManager.DEBUG) {
            return "uuid: " + uuid + "\n" +
                "roleName: " + roleName + "\n" +
                "grantor: " + grantor + "\n" +
                "grantee: " + grantee + "\n" +
                "withadminoption: " + withAdminOption + "\n" +
                "isDef: " + isDef + "\n";
        } else {
            return "";
        }
    }

    public String getDescriptorType()
    {
        return "Role";
    }

    public String getDescriptorName()
    {
        return roleName + " " + grantor + " " + grantee;
    }


    /**
     * Drop this role.descriptor
     *
     * @throws StandardException Could not be dropped.
     */
    public void drop(LanguageConnectionContext lcc) throws StandardException
    {
        DataDictionary dd = getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        dd.dropRoleGrant(roleName, grantee, grantor, tc);
    }

    //////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    //////////////////////////////////////////////

    /**
     * Get the provider's UUID
     *
     * @return The provider's UUID
     */
    public UUID getObjectID()
    {
        return uuid;
    }

    /**
     * Is this provider persistent?  A stored dependency will be required
     * if both the dependent and provider are persistent.
     *
     * @return boolean              Whether or not this provider is persistent.
     */
    public boolean isPersistent()
    {
        return true;
    }

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String   The name of this provider.
     */
    public String getObjectName()
    {
        return ((isDef ? "CREATE ROLE: " : "GRANT ROLE: ") + roleName +
                " GRANT TO: " + grantee +
                " GRANTED BY: " + grantor +
                (withAdminOption? " WITH ADMIN OPTION" : ""));
    }

    /**
     * Get the provider's type.
     *
     * @return char         The provider's type.
     */
    public String getClassType()
    {
        return Dependable.ROLE_GRANT;
    }

    /**
     *  @return the stored form of this provider
     *
     *  @see Dependable#getDependableFinder
     */
    public DependableFinder getDependableFinder()
    {
        return getDependableFinder(StoredFormatIds.ROLE_GRANT_FINDER_V01_ID);
    }
}
