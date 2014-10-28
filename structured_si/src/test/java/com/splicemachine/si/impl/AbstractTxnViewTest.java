package com.splicemachine.si.impl;

import java.io.IOException;
import org.junit.Test;
import org.junit.Assert;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.si.api.Txn;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;

public class AbstractTxnViewTest {

    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER = new ExternalizableSerializer();

	@Test
	public void testSerDe() throws ClassNotFoundException, IOException {		
		ActiveWriteTxn view1 = new  ActiveWriteTxn(1,1);
		Kryo kryo = new Kryo();
		kryo.setReferences(false);
		kryo.setRegistrationRequired(true);
		kryo.register(ActiveWriteTxn.class, EXTERNALIZABLE_SERIALIZER, 12);
		kryo.register(Txn.class, EXTERNALIZABLE_SERIALIZER, 13);
		kryo.register(RootTransaction.class, EXTERNALIZABLE_SERIALIZER, 14);
		Output output = new Output(4096,-1);
		KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
		koo.writeObject(view1);
		output.flush();
		byte[] out = output.toBytes();
		Input input = new Input(out);
		KryoObjectInput koi = new KryoObjectInput(input,kryo);
		ActiveWriteTxn view2 = (ActiveWriteTxn) koi.readObject();		
		Assert.assertTrue("transactions are not equivalent", view1.equals(view2));
	}	
	
}
