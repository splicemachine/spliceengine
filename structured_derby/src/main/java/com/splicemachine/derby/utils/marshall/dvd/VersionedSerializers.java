package com.splicemachine.derby.utils.marshall.dvd;

import org.apache.hadoop.hbase.thrift.generated.Hbase;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class VersionedSerializers {

		private VersionedSerializers(){}

		public static TypeProvider typesForVersion(String version){
				if("2.0".equals(version))
						return V2SerializerMap.instance(true);
				else if("1.0".equals(version))
						return V1SerializerMap.instance(true);
				else
						return latestTypes();
		}

		private static TypeProvider latestTypes() {
				return V2SerializerMap.instance(true);
		}

		public static SerializerMap forVersion(String version,boolean sparse){
				/*
				 * Statically defined version checked versioning
				 */

				if("2.0".equals(version))
						return V2SerializerMap.instance(sparse);
				else if("1.0".equals(version))
						return V1SerializerMap.instance(sparse);

				//when in doubt, assume it's the latest version
				return latestVersion(sparse);
		}

		/**
		 * Get the serializer for the latest encoding version.
		 *
		 * @param sparse whether or not to encode sparsely
		 * @return a serializer map for the latest encoding version.
		 */
		public static SerializerMap latestVersion(boolean sparse){
				return V2SerializerMap.instance(sparse);
		}

}
