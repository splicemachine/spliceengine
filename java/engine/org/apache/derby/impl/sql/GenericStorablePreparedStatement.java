package org.apache.derby.impl.sql;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.StorablePreparedStatement;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.util.ByteArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/**
 * Prepared statement that can be made persistent.
 */
public class GenericStorablePreparedStatement
	extends GenericPreparedStatement implements Formatable, StorablePreparedStatement
{

	// formatable
	private ByteArray 		byteCode;
	private String 			className;

	/**
	 * Niladic constructor, for formatable
	 * only.
	 */
	public GenericStorablePreparedStatement()
	{
		super();
	}

	public GenericStorablePreparedStatement(Statement stmt)
	{
		super(stmt);
	}

	/**
	 * Get our byte code array.  Used
	 * by others to save off our byte
	 * code for us.
	 *
	 * @return the byte code saver
	 */
	public ByteArray getByteCodeSaver()
	{
		if (byteCode == null) {
			byteCode = new ByteArray();
		}
		return byteCode;
	}

	/**
	 * Get and load the activation class.  Will always
	 * return a loaded/valid class or null if the class
	 * cannot be loaded.  
	 *
	 * @return the generated class, or null if the
	 *		class cannot be loaded 
	 *
	 * @exception StandardException on error
	 */
	public GeneratedClass getActivationClass()
		throws StandardException
	{
		if (activationClass == null)
			loadGeneratedClass();

		return activationClass;
	}

	public void setActivationClass(GeneratedClass ac) {

		super.setActivationClass(ac);
		if (ac != null) {
			className = ac.getName();

			// see if this is an pre-compiled class
			if (byteCode != null && byteCode.getArray() == null)
				byteCode = null;
		}
	}

	/////////////////////////////////////////////////////////////
	// 
	// STORABLEPREPAREDSTATEMENT INTERFACE
	// 
	/////////////////////////////////////////////////////////////

	/**
	 * Load up the class from the saved bytes.
	 *
	 * @exception StandardException on error
	 */
	public void loadGeneratedClass()
		throws StandardException
	{
		LanguageConnectionContext lcc =
			(LanguageConnectionContext) ContextService.getContext
				                                  (LanguageConnectionContext.CONTEXT_ID);
		ClassFactory classFactory = lcc.getLanguageConnectionFactory().getClassFactory();

		GeneratedClass gc = classFactory.loadGeneratedClass(className, byteCode);

		/*
		** No special try catch logic to write out bad classes
		** here.  We don't expect any problems, and in any 
		** event, we don't have the class builder available
		** here.
		*/
		setActivationClass(gc);
	}


	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 *
	 * @exception IOException on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(getCursorInfo());
		out.writeBoolean(needsSavepoint());
		out.writeBoolean(isAtomic);
		out.writeObject(executionConstants);
		out.writeObject(resultDesc);

		// savedObjects may be null
		if (savedObjects == null)
		{
			out.writeBoolean(false);
		}
		else
		{	
			out.writeBoolean(true);
			ArrayUtil.writeArrayLength(out, savedObjects);
			ArrayUtil.writeArrayItems(out, savedObjects);
		}

		/*
		** Write out the class name and byte code
		** if we have them.  They might be null if
		** we don't want to write out the plan, and
		** would prefer it just write out null (e.g.
		** we know the plan is invalid).
		*/
		out.writeObject(className);
		out.writeBoolean(byteCode != null);
		if (byteCode != null)
		    byteCode.writeExternal(out);
		
		out.writeBoolean(paramTypeDescriptors!=null);
		if(paramTypeDescriptors !=null){
			ArrayUtil.writeArrayLength(out, paramTypeDescriptors);
			ArrayUtil.writeArrayItems(out, paramTypeDescriptors);
		}
	}

	 
	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		setCursorInfo((CursorInfo)in.readObject());
		setNeedsSavepoint(in.readBoolean());
		isAtomic = (in.readBoolean());
		executionConstants = (ConstantAction) in.readObject();
		resultDesc = (ResultDescription) in.readObject();

		if (in.readBoolean())
		{
			savedObjects = new Object[ArrayUtil.readArrayLength(in)];
			ArrayUtil.readArrayItems(in, savedObjects);
		}

		className = (String)in.readObject();
		if (in.readBoolean()) {
			byteCode = new ByteArray();
			byteCode.readExternal(in);
		}
		else
			byteCode = null;
		
		if(in.readBoolean()){
			paramTypeDescriptors = new DataTypeDescriptor[ArrayUtil.readArrayLength(in)];
			ArrayUtil.readArrayItems(in, paramTypeDescriptors);
		}
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.STORABLE_PREPARED_STATEMENT_V01_ID; }

	/////////////////////////////////////////////////////////////
	// 
	// MISC
	// 
	/////////////////////////////////////////////////////////////
	public boolean isStorable() {
		return true;
	}
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String acn;
			if (activationClass ==null)
				acn = "null";
			else
				acn = activationClass.getName();

 			return "GSPS " + System.identityHashCode(this) + " activationClassName="+acn+" className="+className;
		}
		else
		{
			return "";
		}
	} 
	
	
}

