/*

   Derby - Class org.apache.derby.impl.store.access.PropertyConglomerate

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.derby.impl.store.access;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.gotometrics.orderly.StringRowKey;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.SafeTable;
import com.splicemachine.hbase.filter.ColumnNullableFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.store.access.*;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable; 
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.ShExLockable;
import org.apache.derby.iapi.services.locks.ShExQual;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.store.access.PC_XenaVersion;
import org.apache.derby.impl.store.access.UTF;
import org.apache.derby.impl.store.access.UTFQualifier;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

/**
Stores properties in a congolmerate with complete transactional support.
<p>
The PropertyConglomerate contains one row with 2 columns per property.
Column 0 is the UTF key, and column 1 is the data.
<p>

<p>
The property conglomerate manages the storage of database properties
and thier defaults. Each property is stored as a row in the
PropertyConglomerate 
<OL>
<LI>Column 0 is the UTF key,
<LI>Column 1 is the data.
</OL>
All the property defaults are stored in a single row of the Property
Conglomerate:
<OL>
<LI>Column 0 is the UTF key "derby.defaultPropertyName".
<LI>Column 1 is a FormatableProperties object with one
    row per default property.
</OL>
<p>
In general a propery default defines it value if the property
itself is not defined.

<p>
Because the properties conglomerate is stored in a conglomerate
the information it contains is not available before the raw store
runs recovery. To make a small number of properties (listed in
servicePropertyList) available during early boot, this copies
them to services.properties.
**/
class PropertyConglomerate {
	private static Logger LOG = Logger.getLogger(PropertyConglomerate.class);
    private static final String PROPERTIES_TABLE_NAME = "PROPERTIES";
    private static final byte[] PROPERTIES_TABLE_NAME_BYTES = PROPERTIES_TABLE_NAME.getBytes();
    private static final byte[] VALUE_COLUMN = Integer.toString(1).getBytes();
    protected String propertiesId;
	protected Properties serviceProperties;
	private LockFactory lf;
	private Dictionary	cachedSet;
	private CacheLock cachedLock;

	private PropertyFactory  pf;

    /* Constructors for This class: */

	PropertyConglomerate(
    TransactionController   tc,
    boolean                 create,
    Properties              serviceProperties,
	PropertyFactory 		pf)
		throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("instantiated transaction controller " + tc + ", properties " + serviceProperties);

		
		this.pf = pf;

		if (!create) {
			String id = serviceProperties.getProperty(Property.PROPERTIES_CONGLOM_ID);
			if (id == null) {
				create = true;
			} else {
				try {
					propertiesId = id;
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


            createConglomerate();
//			propertiesConglomId =
//                    createConglomerate(tc,AccessFactoryGlobals.HEAP,
//                            template,
//                            conglomProperties,
//                            TransactionController.IS_DEFAULT);
//                tc.createConglomerate(
//                    AccessFactoryGlobals.HEAP,
//                    template,
//                    null,
//                    (int[]) null, // use default collation for property conglom.
//                    conglomProperties,
//                    TransactionController.IS_DEFAULT);

			serviceProperties.put(
                    Property.PROPERTIES_CONGLOM_ID,
                    PROPERTIES_TABLE_NAME);
		}

		this.serviceProperties = serviceProperties;

		lf = ((SpliceTransactionManager) tc).getAccessManager().getLockFactory();
		cachedLock = new CacheLock(this);

		PC_XenaVersion softwareVersion = new PC_XenaVersion();
		if (create)
			setProperty(tc,DataDictionary.PROPERTY_CONGLOMERATE_VERSION,
						 softwareVersion, true);
		else {
			// XXX - TODO JL FIX
			//softwareVersion.upgradeIfNeeded(tc,this,serviceProperties);
		}
		getCachedDbProperties(tc);
	}

    private void createConglomerate() throws StandardException {
        SpliceLogUtils.trace(LOG,"creating Properties Conglomerate");
        try {
            HBaseAdmin admin = new HBaseAdmin(SpliceUtils.config);
            if(!admin.tableExists(PROPERTIES_TABLE_NAME)){
                HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(PROPERTIES_TABLE_NAME);
                admin.createTable(td);
            }
        } catch (MasterNotRunningException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        } catch (ZooKeeperConnectionException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
    }

    /* Private/Protected methods of This class: */

    /**
     * Create a new PropertyConglomerate row, with values in it.
     **/
    private DataValueDescriptor[] makeNewTemplate(String key, Serializable value)
    {
		if (LOG.isTraceEnabled())
			LOG.trace("makeNewTemplate key " + key + ", value " + value);

		DataValueDescriptor[] template = new DataValueDescriptor[2];

		template[0] = new UTF(key);
		template[1] = new UserType(value);

        return(template);
    }

    /**
     * Create a new empty PropertyConglomerate row, to fetch values into.
     **/
    private DataValueDescriptor[] makeNewTemplate()
    {
		if (LOG.isTraceEnabled())
			LOG.trace("makeNewTemplate ");

		DataValueDescriptor[] template = new DataValueDescriptor[2];

		template[0] = new UTF();
		template[1] = new UserType();

        return(template);
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
 	void setPropertyDefault(TransactionController tc, String key, Serializable value)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("setPropertyDefault TransactionController" + tc + ", key " + key + ", value " + value);

		lockProperties(tc);
		Serializable valueToSave = null;
		//
		//If the default is visible we validate apply and map.
		//otherwise we just map.
		if (propertyDefaultIsVisible(tc,key))
		{
			valueToSave = validateApplyAndMap(tc,key,value,false);
		}
		else
		{
			synchronized (this) {
				Hashtable defaults = new Hashtable();
				getProperties(tc,defaults,false/*!stringsOnly*/,true/*defaultsOnly*/);
				validate(key,value,defaults);
				valueToSave = map(key,value,defaults);
			}
		}
		savePropertyDefault(tc,key,valueToSave);
	}

	boolean propertyDefaultIsVisible(TransactionController tc,String key) throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("propertyDefaultIsVisible " + tc + ", key " + key);

		lockProperties(tc);
		return(readProperty(tc,key) == null);
	}
	
	void saveProperty(TransactionController tc, String key, Serializable value)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("saveProperty " + tc + ", key " + key + ", value " + value);

		if (saveServiceProperty(key,value)) return;

        //see if the property is already there
        HTableInterface table = null;
        try{
            table = SpliceAccessManager.getHTable(PROPERTIES_TABLE_NAME_BYTES);

            byte[] keyBytes = key.getBytes();
            byte[] valColumn = Integer.toString(1).getBytes();
            String transactionId = getTransactionId(tc);
            if(value==null){
                //null value means delete the property
                Delete delete = new Delete(keyBytes);

                if(transactionId!=null) {
                    SpliceUtils.getTransactionGetsPuts().prepDelete(transactionId, delete);
                }

                table.delete(delete);
            }

            //set the value
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(value);

            Put put = SpliceUtils.createPut(transactionId, keyBytes);
            put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, valColumn,baos.toByteArray());
            if(transactionId!=null) {
                SpliceUtils.getTransactionGetsPuts().prepPut(transactionId, put);
            }

            table.put(put);
        }catch(IOException ioe){
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe);
        }finally{
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    SpliceLogUtils.error(LOG,"Unable to close property table",e);
                }
            }
        }

	}

    private String getTransactionId(TransactionController tc) {
        Transaction td = ((SpliceTransactionManager)tc).getRawTransaction();
        return SpliceUtils.getTransID(td);
    }

    private boolean saveServiceProperty(String key, Serializable value)
	{
		if (LOG.isTraceEnabled())
			LOG.trace("saveServiceProperty key " + key + ", value " + value);

		if (PropertyUtil.isServiceProperty(key))
		{
			if (value != null)
				serviceProperties.put(key, value);
			else
				serviceProperties.remove(key);
			return true;
		}
		else
		{
			return false;
		}
	}

	void savePropertyDefault(TransactionController tc, String key, Serializable value)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("savePropertyDefault TransactionController " + tc + ", key " + key + ", value " + value);
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

	private Serializable validateApplyAndMap(TransactionController tc,
											 String key, Serializable value, boolean dbOnlyProperty)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("validateApplyAndMap TransactionController " + tc + ", key " + key + ", value " + value);
		Dictionary d = new Hashtable();
		getProperties(tc,d,false/*!stringsOnly*/,false/*!defaultsOnly*/);
		Serializable mappedValue = pf.doValidateApplyAndMap(tc, key,
																   value, d, dbOnlyProperty);
		//
		// RESOLVE: log device cannot be changed on the fly right now
		if (key.equals(Attribute.LOG_DEVICE))
        {
			throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CHANGE_LOGDEVICE);
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
	private Serializable map(String key,
							 Serializable value,
							 Dictionary set)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("map key " + key + ", value " + value);
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

	private void validate(String key,
						  Serializable value,
						  Dictionary set)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("validate key " + key + ", value " + value);
		pf.validateSingleProperty(key, value, set);
	}


	private boolean bootPasswordChange(TransactionController tc,
									   String key,
									   Serializable value)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("bootPasswordChange transactionController " + tc + ", key " + key + ", value " + value);

		// first check for boot password  change - we don't put boot password
		// in the servicePropertyList because if we do, then we expose the
		// boot password in clear text
		if (key.equals(Attribute.BOOT_PASSWORD))
		{
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
		else
		{
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
	void setProperty(
    TransactionController tc,
    String                key,
    Serializable          value, boolean dbOnlyProperty)
		throws StandardException
    {
		if (LOG.isTraceEnabled())
			LOG.trace("setProperty transactionController " + tc + ", key " + key + ", value " + value);

		if (SanityManager.DEBUG)
        {

			if (!((value == null) || (value instanceof Formatable)))
            {
                if (!(value.getClass().getName().startsWith("java.")))
                {
                    SanityManager.THROWASSERT(
                        "Non-formattable, non-java class - " +
                        value.getClass().getName());
                }
            }
		}

		lockProperties(tc);
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

	private Serializable readProperty(TransactionController tc,
									  String key) throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("readProperty transactionController " + tc + ", key " + key);

		// scan the table for a row with matching "key"
        HTableInterface table = SpliceAccessManager.getHTable(PROPERTIES_TABLE_NAME_BYTES);
        try {
            Get get = SpliceUtils.createGet(getTransactionId(tc), new StringRowKey().serialize(key));
            get.addColumn(HBaseConstants.DEFAULT_FAMILY_BYTES,VALUE_COLUMN);

            Result result = table.get(get);

            if(result==null||result.isEmpty()) return null;
            return getValue(result.getValue(HBaseConstants.DEFAULT_FAMILY_BYTES,VALUE_COLUMN));
        } catch (IOException e) {
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        } catch (ClassNotFoundException e) {
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        } finally{
            try {
                table.close();
            } catch (IOException e) {
                SpliceLogUtils.error(LOG,"Unable to close properties table",e);
            }
        }
	}

    private Serializable getValue(byte[] value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(value);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (Serializable)ois.readObject();
    }

    private Serializable getCachedProperty(TransactionController tc,
										   String key) throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("getCachedProperty transactionController " + tc + ", key " + key);

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

	private Serializable getCachedPropertyDefault(TransactionController tc,
												  String key,
												  Dictionary dbProps)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("getCachedPropertyDefault transactionController " + tc + ", key " + key);

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
	Serializable getProperty(
    TransactionController tc, 
    String                key) 
		throws StandardException
    {
		if (LOG.isTraceEnabled())
			LOG.trace("getProperty transactionController " + tc + ", key " + key);

		//
		//Try service properties first.
		if(PropertyUtil.isServiceProperty(key)) return serviceProperties.getProperty(key);

		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
		if (iHoldTheUpdateLock(tc))
		{
			//
			//Return the property value if it is defined.
			Serializable v = readProperty(tc,key);
			if (v != null) return v;

			return getPropertyDefault(tc,key);
		}
		else
		{
			return getCachedProperty(tc,key);
		}
	}

	/**
	 * Get the default for a property.
	 * @exception  StandardException  Standard exception policy.
	 */
	Serializable getPropertyDefault(TransactionController tc, String key)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("getPropertyDefault transactionController " + tc + ", key " + key);

		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
		if (iHoldTheUpdateLock(tc))
		{
			//
			//Return the property default value (may be null) if
			//defined.
			Dictionary defaults = (Dictionary)readProperty(tc,AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
			if (defaults == null)
				return null;
			else
				return (Serializable)defaults.get(key);
		}
		else
		{
			return getCachedPropertyDefault(tc,key,null);
		}
	}
									
	private Dictionary copyValues(Dictionary to, Dictionary from, boolean stringsOnly)
	{
		if (LOG.isTraceEnabled())
			LOG.trace("copyValues ");

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
	Properties getProperties(TransactionController tc) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("getProperties transactionController " + tc);

		Properties p = new Properties();
		getProperties(tc,p,true/*stringsOnly*/,false/*!defaultsOnly*/);
		return p;
	}

	public void getProperties(TransactionController tc,
							   Dictionary d,
							   boolean stringsOnly,
							   boolean defaultsOnly) throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("getProperties transactionController " + tc);

		// See if I'm the exclusive owner. If so I cannot populate
		// the cache as it would contain my uncommitted changes.
		if (iHoldTheUpdateLock(tc))
		{
			Dictionary dbProps = readDbProperties(tc);
			Dictionary defaults = (Dictionary)dbProps.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
			copyValues(d,defaults,stringsOnly);
			if (!defaultsOnly)copyValues(d,dbProps,stringsOnly);
		}
		else
		{	
			Dictionary dbProps = getCachedDbProperties(tc);
			Dictionary defaults = (Dictionary)dbProps.get(AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
			copyValues(d,defaults,stringsOnly);
			if (!defaultsOnly)copyValues(d,dbProps,stringsOnly);
		}
	}

	void resetCache() {cachedSet = null;}

	/** Read the database properties and add in the service set. */
	private Dictionary readDbProperties(TransactionController tc)
		 throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("readDbProperties transactionController " + tc);

		Dictionary set = new Hashtable();

        // scan the table for a row with no matching "key"
        HTableInterface table = SpliceAccessManager.getHTable(PROPERTIES_TABLE_NAME_BYTES);

        Scan scan = new Scan();
        scan.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);
        scan.setCaching(100);
        scan.addColumn(HBaseConstants.DEFAULT_FAMILY_BYTES,VALUE_COLUMN);
//        scan.setFilter(new ColumnNullableFilter(HBaseConstants.DEFAULT_FAMILY_BYTES,VALUE_COLUMN,
//                CompareFilter.CompareOp.GREATER_OR_EQUAL));
        try{
            ResultScanner scanner = table.getScanner(scan);
            Result result;
            while((result = scanner.next())!=null){
                if(result.isEmpty()){
                    break;
                }
                String key = Bytes.toString(result.getRow());
                byte[] valBytes = result.getColumnLatest(HBaseConstants.DEFAULT_FAMILY_BYTES,VALUE_COLUMN).getValue();
                Object value = getValue(valBytes);
                set.put(key,value);
            }
        } catch (IOException e) {
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        } catch (ClassNotFoundException e) {
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        } finally{
            try{
                if(table!=null) table.close();
            }catch(IOException e){
                SpliceLogUtils.error(LOG,"Unable to close properties table",e);
            }
        }

		// add the known properties from the service properties set
		for (int i = 0; i < PropertyUtil.servicePropertyList.length; i++) {
			String value =
				serviceProperties.getProperty(PropertyUtil.servicePropertyList[i]);
			if (value != null) set.put(PropertyUtil.servicePropertyList[i], value);
		}
		return set;
	}

	private Dictionary getCachedDbProperties(TransactionController tc)
		 throws StandardException
	{
		Dictionary dbProps = cachedSet;
		//Get the cached set of properties.
		if (dbProps == null)
		{
			dbProps = readDbProperties(tc);
			cachedSet = dbProps;
		}
		
		return dbProps;
	}

	/** Lock the database properties for an update. */
	void lockProperties(TransactionController tc) throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("lockProperties transactionController " + tc);

		// lock the property set until the transaction commits.
		// This allows correct operation of the cache. The cache remains
		// valid for all transactions except the one that is modifying
		// it. Thus readers see the old uncommited values. When this
		// thread releases its exclusive lock the cached is cleared
		// and the next reader will re-populate the cache.
		CompatibilitySpace cs = tc.getLockSpace();
		Object csGroup = cs.getOwner();
		lf.lockObject(cs, csGroup, cachedLock, ShExQual.EX,
					  C_LockFactory.TIMED_WAIT);
	}

	/**
	  Return true if the caller holds the exclusive update lock on the
	  property conglomerate.
	  */
	private boolean iHoldTheUpdateLock(TransactionController tc) throws StandardException
	{
		if (LOG.isTraceEnabled())
			LOG.trace("iHoldTheUpdateLock transactionController " + tc);

		CompatibilitySpace cs = tc.getLockSpace();
		Object csGroup = cs.getOwner();
		return lf.isLockHeld(cs, csGroup, cachedLock, ShExQual.EX);
	}
}

/**
	Only used for exclusive lock purposes.
*/
class CacheLock extends ShExLockable {

	private PropertyConglomerate pc;

	CacheLock(PropertyConglomerate pc) {
		this.pc = pc;
	}

	public void unlockEvent(Latch lockInfo)
	{
		super.unlockEvent(lockInfo);		
		pc.resetCache();
	}
}
