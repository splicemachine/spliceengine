/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 */

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.*;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.RawStoreFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.types.UserType;
import com.splicemachine.db.impl.sql.execute.GenericScanQualifier;
import com.splicemachine.db.impl.store.access.PC_XenaVersion;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.PropertyManagerService;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

public class PropertyConglomerate {
	private static Logger LOG = Logger.getLogger(PropertyConglomerate.class);
	protected long propertiesConglomId;
	protected Properties serviceProperties;
	protected Properties loadedProperties;
	private Dictionary	cachedSet;
	private Dictionary	cachedStoreSet;
	private PropertyFactory  pf;

    /* Constructors for This class: */

	public PropertyConglomerate(TransactionController tc, boolean create, Properties properties, PropertyFactory pf) throws StandardException {
		serviceProperties = new Properties();
		PropertyManager propertyManager=PropertyManagerService.loadPropertyManager();
		this.pf = pf;
		if (!create) {

			propertyManager.getProperty(Property.PROPERTIES_CONGLOM_ID);
			String id = propertyManager.getProperty(Property.PROPERTIES_CONGLOM_ID);
			if (id == null) {
				create = true;
			} else {
				try {
					propertiesConglomId =Long.parseLong(id);
				} catch (NumberFormatException nfe) {
					throw Monitor.exceptionStartingModule(nfe) ;
				}
			}
		}
		
		if (create) {
			DataValueDescriptor[] template = makeNewTemplate();
			Properties conglomProperties = new Properties();
			conglomProperties.put(
                Property.PAGE_SIZE_PARAMETER, 
                RawStoreFactory.PAGE_SIZE_STRING);
			conglomProperties.put(
                RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, 
                RawStoreFactory.PAGE_RESERVED_ZERO_SPACE_STRING);
			propertiesConglomId = 
                tc.createConglomerate(false,
                    AccessFactoryGlobals.HEAP,
                    template, 
                    null,
						null, // use default collation for property conglom.
                    conglomProperties, 
                    TransactionController.IS_DEFAULT);

			//
			// IMPORTANT: Hey, you!  Yeah, you!  Before you think about adding another "service" property default here,
			// make sure that you add it to PropertyUtil.servicePropertyList.  If you don't, you will never be able to override the default.  Ouch!
			//
			serviceProperties.put(Property.PROPERTIES_CONGLOM_ID, Long.toString(propertiesConglomId));
			serviceProperties.put(Property.ROW_LOCKING, "false");
			serviceProperties.put("derby.language.logStatementText", "false");
			serviceProperties.put("derby.language.logQueryPlan", "false");
			serviceProperties.put(Property.LOCKS_ESCALATION_THRESHOLD, "500");
			serviceProperties.put(Property.DATABASE_PROPERTIES_ONLY, "false");
			serviceProperties.put(Property.DEFAULT_CONNECTION_MODE_PROPERTY, "fullAccess");
			for (Object key: serviceProperties.keySet()) {
				propertyManager.addProperty((String)key,serviceProperties.getProperty((String)key));
			}
		}

		try {
			Set<String> props = propertyManager.listProperties();
			for (String child: props) {
				String value = propertyManager.getProperty(child);
				serviceProperties.put(child, value);
			}
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "getServiceProperties Failed", Exceptions.parseException(e));
		}
		PC_XenaVersion softwareVersion = new PC_XenaVersion();
		if (create)
			setProperty(tc,DataDictionary.PROPERTY_CONGLOMERATE_VERSION,softwareVersion, true);
		getCachedProperties(tc);
	}

    /* Private/Protected methods of This class: */

    /**
     * Create a new PropertyConglomerate row, with values in it.
     **/
    public DataValueDescriptor[] makeNewTemplate(String key, Serializable value) {
		DataValueDescriptor[] template = new DataValueDescriptor[2];
		template[0] = new SQLVarchar(key);
		template[1] = new UserType(value);
        return(template);
    }

    /**
     * Create a new empty PropertyConglomerate row, to fetch values into.
     **/
    public DataValueDescriptor[] makeNewTemplate() {
		DataValueDescriptor[] template = new DataValueDescriptor[2];
		template[0] = new SQLVarchar();
		template[1] = new UserType();
        return(template);
    }

    /**
     * Open a scan on the properties conglomerate looking for "key".
     * <p>
	 * Open a scan on the properties conglomerate qualified to
	 * find the row with value key in column 0.  Both column 0
     * and column 1 are included in the scan list.
     *
	 * @return an open ScanController on the PropertyConglomerate. 
     *
	 * @param tc        The transaction to do the Conglomerate work under.
     * @param key       The "key" of the property that is being requested.
     * @param open_mode Whether we are setting or getting the property.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public ScanController openScan(TransactionController tc, String key, int open_mode) throws StandardException {
		Qualifier[][] qualifiers = null;
		if (key != null) { 			// Set up qualifier to look for the row with key value in column[0]
			qualifiers = new Qualifier[1][];
            qualifiers[0] = new Qualifier[1];
            GenericScanQualifier gsq = new GenericScanQualifier();
            gsq.setQualifier(0, new SQLVarchar(key),DataValueDescriptor.ORDER_OP_EQUALS, false, false, false);
			qualifiers[0][0] = gsq;
		}
		return(tc.openScan(
            propertiesConglomId,
            false, // don't hold over the commit
            open_mode,
            TransactionController.MODE_TABLE,
            TransactionController.ISOLATION_SERIALIZABLE,
            (FormatableBitSet) null,
            (DataValueDescriptor[]) null,	// start key
            ScanController.NA,
            qualifiers,
            (DataValueDescriptor[]) null,	// stop key
            ScanController.NA));
	}
    /* Package Methods of This class: */

	/**
		Set a property in the conglomerate.

	 @param	key		The key used to lookup this property.
	 @param	value	The value to be associated with this key. If null, delete the
	 					property from the properties list.
	*/

	/**
	 * Set the default for a property.
	 * @exception  StandardException  Standard exception policy.
	 */
    public void setPropertyDefault(TransactionController tc, String key, Serializable value) throws StandardException {
		Serializable valueToSave = null;
		//
		//If the default is visible we validate apply and map.
		//otherwise we just map.
		if (propertyDefaultIsVisible(tc,key)) {
			valueToSave = validateApplyAndMap(tc,key,value,false);
		}
		else {
			synchronized (this) {
				Hashtable defaults = new Hashtable();
				getProperties(tc,defaults,false/*!stringsOnly*/,true/*defaultsOnly*/);
				validate(key,value,defaults);
				valueToSave = map(key,value,defaults);
			}
		}
		savePropertyDefault(tc,key,valueToSave);
	}

    public boolean propertyDefaultIsVisible(TransactionController tc,String key) throws StandardException {
		return(readProperty(tc,key) == null);
	}
	
    public void saveProperty(TransactionController tc, String key, Serializable value) throws StandardException {
		if (saveServiceProperty(key,value)) return;
        // Do a scan to see if the property already exists in the Conglomerate.
		ScanController scan = this.openScan(tc, key, TransactionController.OPENMODE_FORUPDATE);

        DataValueDescriptor[] row = makeNewTemplate();
		if (scan.fetchNext(row)) {
			if (value == null) {
				// A null input value means that we should delete the row
				scan.delete();
			} 
            else {
				// a value already exists, just replace the second columm
				row[1] = new UserType(value);
				scan.replace(row, (FormatableBitSet) null);
			}
			scan.close();
		}
        else {
            // The value does not exist in the Conglomerate.
            scan.close();
            scan = null;
            if (value != null) {
                // not a delete request, so insert the new property.
                row = makeNewTemplate(key, value);
                ConglomerateController cc = 
                    tc.openConglomerate(
                        propertiesConglomId, 
                        false,
                        TransactionController.OPENMODE_FORUPDATE, 
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);

                cc.insert(row);
                cc.close();
            }
        }
	}

    public boolean saveServiceProperty(String key, Serializable value) {
		if (PropertyUtil.isServiceProperty(key)) {
			if (value != null)
				serviceProperties.put(key, value);
			else
				serviceProperties.remove(key);
			return true;
		}
		else {
			return false;
		}
	}

    public void savePropertyDefault(TransactionController tc, String key, Serializable value) throws StandardException {
		if (saveServiceProperty(key,value)) return;

		Dictionary defaults = (Dictionary)readProperty(tc,AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
		if (defaults == null) defaults = new FormatableHashtable();
		if (value==null)
			defaults.remove(key);
		else
			defaults.put(key,value);
		if (defaults.size() == 0) defaults = null;
		saveProperty(tc,AccessFactoryGlobals.DEFAULT_PROPERTY_NAME,(Serializable)defaults);
	}

    public Serializable validateApplyAndMap(TransactionController tc, String key, Serializable value, boolean dbOnlyProperty) throws StandardException {
		Dictionary d = new Hashtable();
		getProperties(tc,d,false/*!stringsOnly*/,false/*!defaultsOnly*/);
		Serializable mappedValue = pf.doValidateApplyAndMap(tc, key,value, d, dbOnlyProperty);
		//
		// RESOLVE: log device cannot be changed on the fly right now
		if (key.equals(Attribute.LOG_DEVICE)) {
			throw StandardException.newException(SQLState.RAWSTORE_CANNOT_CHANGE_LOGDEVICE);
        }

		if (mappedValue == null)
			return value;
		else
			return mappedValue;
	}

	/**
	  Call the property set callbacks to map a proposed property value
	  to a value to save.
	  <P>
	  The caller must run this in a block synchronized on this
	  to serialize validations with changes to the set of
	  property callbacks
	  */
    public Serializable map(String key, Serializable value, Dictionary set) throws StandardException {
		return pf.doMap(key, value, set);
	}

	/**
	  Call the property set callbacks to validate a property change
	  against the property set provided.
	  <P>
	  The caller must run this in a block synchronized on this
	  to serialize validations with changes to the set of
	  property callbacks
	  */

    public void validate(String key, Serializable value, Dictionary set) throws StandardException {
		pf.validateSingleProperty(key, value, set);
	}


    public boolean bootPasswordChange(TransactionController tc, String key, Serializable value) throws StandardException {
		// first check for boot password  change - we don't put boot password
		// in the servicePropertyList because if we do, then we expose the
		// boot password in clear text
		if (key.equals(Attribute.BOOT_PASSWORD)) {
			// The user is trying to change the secret key.
			// The secret key is never stored in clear text, but we
			// store the encrypted form in the services.properties
			// file.  Swap the secret key with the encrypted form and
			// put that in the services.properties file.
			AccessFactory af = ((TransactionManager)tc).getAccessManager();

			RawStoreFactory rsf = (RawStoreFactory)
				Monitor.findServiceModule(af, RawStoreFactory.MODULE);

			// remove secret key from properties list if possible
			serviceProperties.remove(Attribute.BOOT_PASSWORD);

			value = rsf.changeBootPassword(serviceProperties, value);
			serviceProperties.put(RawStoreFactory.ENCRYPTED_KEY,value);
			return true;
		}
		else {
			return false;
		}
	}

    /**
     * Sets the Serializable object associated with a property key.
     * <p>
     * This implementation turns the setProperty into an insert into the
     * PropertyConglomerate conglomerate.
     * <p>
     * See the discussion of getProperty().
     * <p>
     * The value stored may be a Formatable object or a Serializable object
	 * whose class name starts with java.*. This stops arbitary objects being
	 * stored in the database by class name, which will cause problems in
	 * obfuscated/non-obfuscated systems.
     *
	 * @param	tc		The transaction to do the Conglomerate work under.
	 * @param	key		The key used to lookup this property.
	 * @param	value	The value to be associated with this key. If null,
     *                  delete the property from the properties list.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void setProperty(TransactionController tc, String key, Serializable value, boolean dbOnlyProperty) throws StandardException {
		if (SanityManager.DEBUG) {
			if (!((value == null) || (value instanceof Formatable))) {
                if (!(value.getClass().getName().startsWith("java."))) {
                    SanityManager.THROWASSERT(
                        "Non-formattable, non-java class - " +
                        value.getClass().getName());
                }
            }
		}

		Serializable valueToValidateAndApply = value;
		//
		//If we remove a property we validate and apply its default.
		if (value == null)
			valueToValidateAndApply = getPropertyDefault(tc,key);
		Serializable valueToSave =
			validateApplyAndMap(tc,key,valueToValidateAndApply, dbOnlyProperty);

		//
		//if this is a bootPasswordChange we save it in
		//a special way.
		if (bootPasswordChange(tc,key,value))
			return;

		//
		//value==null means we are removing a property.
		//To remove the property we call saveProperty with
		//a null value. Note we avoid saving the mapped
		//DEFAULT value returned by validateAndApply.
		else if (value==null)
			saveProperty(tc,key,null);
		//
		//value != null means we simply save the possibly
		//mapped value of the property returned by
		//validateAndApply.
		else
			saveProperty(tc,key,valueToSave);
	}

    public Serializable readProperty(TransactionController tc, String key) throws StandardException {
		// scan the table for a row with matching "key"
		ScanController scan = openScan(tc, key, 0);
		DataValueDescriptor[] row = makeNewTemplate();
		// did we find at least one row?
		boolean isThere = scan.fetchNext(row);
		scan.close();
		if (!isThere) return null;
		return (Serializable) (((UserType) row[1]).getObject());
	}

    public Serializable getCachedProperty(TransactionController tc, String key) throws StandardException {
		//
		//Get the cached set of properties.
		Dictionary dbProps = getCachedProperties(tc);

		//
		//Return the value if it is defined.
		if (dbProps.get(key) != null)
			return (Serializable) dbProps.get(key);
		else
			return getCachedPropertyDefault(tc,key,dbProps);
	}

    public Serializable getCachedPropertyDefault(TransactionController tc, String key, Dictionary dbProps) throws StandardException {
		//
		//Get the cached set of properties.
		if (dbProps == null) dbProps = getCachedProperties(tc);
		//
		//return the default for the value if it is defined.
		Dictionary defaults = (Dictionary)dbProps.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
		if (defaults == null)
			return null;
		else
			return (Serializable)defaults.get(key);
	}

    /**
     * Gets the de-serialized object associated with a property key.
     * <p>
     * The Store provides a transaction protected list of database properties.
     * Higher levels of the system can store and retrieve these properties
     * once Recovery has finished. Each property is a serializable object
     * and is stored/retrieved using a String key.
     * <p>
     * In this implementation a lookup is done on the PropertyConglomerate
     * conglomerate, using a scan with "key" as the qualifier.
     * <p>
	 * @param tc      The transaction to do the Conglomerate work under.
     * @param key     The "key" of the property that is being requested.
     *
	 * @return object The object associated with property key. n
     *                ull means no such key-value pair.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Serializable getProperty(TransactionController tc, String key)  throws StandardException {

		//
		//Try service properties first.
		if(PropertyUtil.isServiceProperty(key) || serviceProperties.containsKey(key))
			return serviceProperties.getProperty(key);
		//
		//Return the property value if it is defined.
		Serializable v = readProperty(tc,key);
		if (v != null) return v;
		v =  getPropertyDefault(tc,key);
		return v;
	}

	/**
	 * Get the default for a property.
	 * @exception  StandardException  Standard exception policy.
	 */
    public Serializable getPropertyDefault(TransactionController tc, String key) throws StandardException {
		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
			//
			//Return the property default value (may be null) if
			//defined.
			Dictionary defaults = (Dictionary)readProperty(tc,AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
			if (defaults == null)
				return null;
			else
				return (Serializable)defaults.get(key);
	}
									
    public Dictionary copyValues(Dictionary to, Dictionary from, boolean stringsOnly) {    	
		if (from == null) return to; 
		for (Enumeration keys = from.keys(); keys.hasMoreElements(); ) {
			String key = (String) keys.nextElement();
			Object value = from.get(key);
			if ((value instanceof String) || !stringsOnly)
				to.put(key, value);
		}
		return to;
	}

	/**
		Fetch the set of properties as a Properties object. This means
		that only keys that have String values will be included.
	*/
    public Properties getProperties(TransactionController tc) throws StandardException {

		Properties p = new Properties();
		getProperties(tc,p,true/*stringsOnly*/,false/*!defaultsOnly*/);		
		return p;
	}

	public void getProperties(TransactionController tc, Dictionary d, boolean stringsOnly, boolean defaultsOnly) throws StandardException {
		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
		Dictionary props = getCachedProperties(tc);
		Dictionary defaults = (Dictionary)props.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
		copyValues(d,defaults,stringsOnly);
		if (!defaultsOnly)
			copyValues(d,props,stringsOnly);
	}

	/**
	 * Fetch the set of store properties as a Properties object. This means
	 * that only keys that have String values will be included.
	 */
	public Properties getStoreProperties(TransactionController tc) throws StandardException {

		Properties p = new Properties();
		getStoreProperties(tc,p,true/*stringsOnly*/,false/*!defaultsOnly*/);		
		return p;
	}

	public void getStoreProperties(TransactionController tc, Dictionary d, boolean stringsOnly, boolean defaultsOnly) throws StandardException {
		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
		Dictionary props = getCachedStoreProperties(tc);
		Dictionary defaults = (Dictionary)props.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
		copyValues(d,defaults,stringsOnly);
		if (!defaultsOnly)
			copyValues(d,props,stringsOnly);
	}

	/**
	 * Fetch the set of service properties as a Properties object. This means
	 * that only keys that have String values will be included.
	 */
	public Properties getServiceProperties(TransactionController tc) throws StandardException {

		Properties p = new Properties();
		getServiceProperties(tc,p,true/*stringsOnly*/,false/*!defaultsOnly*/);		
		return p;
	}

	public void getServiceProperties(TransactionController tc, Dictionary d, boolean stringsOnly, boolean defaultsOnly) throws StandardException {
		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
		Dictionary props = getCachedServiceProperties(tc);
		Dictionary defaults = (Dictionary)props.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
		copyValues(d,defaults,stringsOnly);
		if (!defaultsOnly)
			copyValues(d,props,stringsOnly);
	}

	void resetCache() {cachedSet = null;}

	/** Read the database (store) properties and add in the service set. */
	private Dictionary readProperties(TransactionController tc) throws StandardException {
		Dictionary set = readStoreProperties(tc);

		// add the known properties from the service properties set
		Dictionary serviceSet = readServiceProperties(tc);
		for (Enumeration e = serviceSet.keys(); e.hasMoreElements();) {
			Object key = e.nextElement();
			set.put(key, serviceSet.get(key));
		}

		return set;
	}

	/**
	 * Read the database properties from the store.  (Do not read the service properties.)
	 * @param tc
	 * @return set
	 * @throws StandardException
	 */
	private Dictionary readStoreProperties(TransactionController tc) throws StandardException {
		Dictionary set = new Hashtable();

		// scan the table for a row with no matching "key"
		ScanController scan = openScan(tc, (String) null, 0);

		DataValueDescriptor[] row = makeNewTemplate();

		while (scan.fetchNext(row)) {
			Object key = ((SQLVarchar) row[0]).getObject();
			Object value = ((UserType) row[1]).getObject();
			if (SanityManager.DEBUG) {
                if (!(key instanceof String))
                    SanityManager.THROWASSERT(
                        "Key is not a string " + key.getClass().getName());
			}
			set.put(key, value);
		}
		scan.close();
		return set;
	}

	/**
	 * Read only the service properties from the store.
	 * @param tc
	 * @return set
	 * @throws StandardException
	 */
	private Dictionary readServiceProperties(TransactionController tc) throws StandardException {
		Dictionary set = new Hashtable();

		// add the known properties from the service properties set
		for (int i = 0; i < PropertyUtil.servicePropertyList.length; i++) {
			String value =
				serviceProperties.getProperty(PropertyUtil.servicePropertyList[i]);
			if (value != null) set.put(PropertyUtil.servicePropertyList[i], value);
		}
		return set;
	}

	private Dictionary getCachedProperties(TransactionController tc) throws StandardException {
		Dictionary props = cachedSet;
		// Get the cached set of properties.
		if (props == null) {
			props = readProperties(tc);
			cachedSet = props;
		}
		return props;
	}

	private Dictionary getCachedStoreProperties(TransactionController tc) throws StandardException {
		Dictionary props = cachedStoreSet;
		// Get the cached set of store properties.
		if (props == null) {
			props = readStoreProperties(tc);
			cachedStoreSet = props;
		}
		return props;
	}

	private Dictionary getCachedServiceProperties(TransactionController tc) throws StandardException {
		// Just call the read method since the service properties are loaded in the constructor
		// and its durable storage (ZooKeeper, in Splice's case) is considered immutable at this time.
		// This is most likely due to the history of Derby's service.properties file where these
		// service properties lived in the past and it would have been difficult to update this file previously.
		// In the future, Splice may allow for updates to the service properties within ZooKeeper.
		return readServiceProperties(tc);
	}
}

/**
	Only used for exclusive lock purposes.
*/
