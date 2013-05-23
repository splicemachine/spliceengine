package com.splicemachine.derby.impl.store.access;

import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import org.apache.derby.impl.store.access.PC_XenaVersion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.ZkUtils;

public class PropertyConglomerate {
	private static Logger LOG = Logger.getLogger(PropertyConglomerate.class);
	protected long propertiesConglomId;
	protected Properties serviceProperties;
	protected Properties loadedProperties;
	private Dictionary	cachedSet;
	private PropertyFactory  pf;

    /* Constructors for This class: */

	public PropertyConglomerate(TransactionController tc, boolean create, Properties properties, PropertyFactory pf) throws StandardException {
		serviceProperties = new Properties();
		this.pf = pf;
		if (!create) {
			
			SpliceUtils.getProperty(Property.PROPERTIES_CONGLOM_ID);
			String id = Bytes.toString(SpliceUtils.getProperty(Property.PROPERTIES_CONGLOM_ID));
			if (id == null) {
				create = true;
			} else {
				try {
					propertiesConglomId = Long.valueOf(id).longValue();
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
                tc.createConglomerate(
                    AccessFactoryGlobals.HEAP,
                    template, 
                    null, 
                    (int[]) null, // use default collation for property conglom.
                    conglomProperties, 
                    TransactionController.IS_DEFAULT);

			serviceProperties.put(Property.PROPERTIES_CONGLOM_ID, Long.toString(propertiesConglomId));
			serviceProperties.put("derby.storage.rowLocking", "false");
			serviceProperties.put("derby.language.logStatementText", "false");
			serviceProperties.put("derby.language.logQueryPlan", "false");
			serviceProperties.put("derby.locks.escalationThreshold", "500");
			serviceProperties.put("derby.database.propertiesOnly", "false");
			serviceProperties.put("derby.database.defaultConnectionMode", "fullAccess");
			for (Object key: serviceProperties.keySet()) {
				SpliceUtils.addProperty((String)key, serviceProperties.getProperty((String)key));
			}
		}

		try {
			List<String> children = ZkUtils.getChildren(SpliceConstants.zkSpliceDerbyPropertyPath, false);
			for (String child: children) {
				String value = Bytes.toString(ZkUtils.getData(SpliceConstants.zkSpliceDerbyPropertyPath + "/" + child));
				serviceProperties.put(child, value);
			}
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "getServiceProperties Failed", Exceptions.parseException(e));
		}
		this.serviceProperties = serviceProperties;
		PC_XenaVersion softwareVersion = new PC_XenaVersion();
		if (create)
			setProperty(tc,DataDictionary.PROPERTY_CONGLOMERATE_VERSION,softwareVersion, true);
		getCachedDbProperties(tc);
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
		ScanController scan =         // open the scan, clients will do the fetches and close.
            tc.openScan(
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
                ScanController.NA);
		return(scan);
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
		Dictionary dbProps = getCachedDbProperties(tc);

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
		if (dbProps == null) dbProps = getCachedDbProperties(tc);
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
		if(PropertyUtil.isServiceProperty(key) || serviceProperties.containsKey(key) ||
				key.equals("derby.database.fullAccessUsers") || key.equals("derby.database.readOnlyAccessUsers"))
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
		Dictionary dbProps = getCachedDbProperties(tc);
		Dictionary defaults = (Dictionary)dbProps.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
		copyValues(d,defaults,stringsOnly);
		if (!defaultsOnly)
			copyValues(d,dbProps,stringsOnly);
		
//			Dictionary dbProps = readDbProperties(tc);
//			Dictionary defaults = (Dictionary)dbProps.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
//			copyValues(d,defaults,stringsOnly);
//			if (!defaultsOnly)copyValues(d,dbProps,stringsOnly);
		
	}

	public void resetCache() {cachedSet = null;}

	/** Read the database properties and add in the service set. */
	public Dictionary readDbProperties(TransactionController tc) throws StandardException {
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

		// add the known properties from the service properties set
		for (int i = 0; i < PropertyUtil.servicePropertyList.length; i++) {
			String value =
				serviceProperties.getProperty(PropertyUtil.servicePropertyList[i]);
			if (value != null) set.put(PropertyUtil.servicePropertyList[i], value);
		}
		return set;
	}

	public Dictionary getCachedDbProperties(TransactionController tc) throws StandardException {
		Dictionary dbProps = cachedSet;
		//Get the cached set of properties.
		if (dbProps == null) {
			dbProps = readDbProperties(tc);
			cachedSet = dbProps;
		}
		return dbProps;
	}

}

/**
	Only used for exclusive lock purposes.
*/


