package com.splicemachine.derby.utils.marshall.dvd;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class VersionedSerializers {

		private VersionedSerializers(){}

		public static TypeProvider typesForVersion(String version){
				if("2.0".equals(version))
						return V2SerializerMap.instance(true);
				else
						return V1SerializerMap.instance(true);
		}

		public static SerializerMap forVersion(String version,boolean sparse){
				/*
				 * Statically defined version checked versioning
				 */

				if("2.0".equals(version))
						return V2SerializerMap.instance(sparse);

				//when in doubt, assume it's encoded using V1.
				return V1SerializerMap.instance(sparse);
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
