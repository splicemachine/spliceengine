package com.splicemachine.spark2.splicemachine

object SparkVersionSpecificItems {
  
  val schemaWithMetadata = "{\"type\":\"struct\",\"fields\":[{\"name\":\"C1_BOOLEAN\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{\"name\":\"C1_BOOLEAN\",\"scale\":0}},{\"name\":\"C2_CHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C2_CHAR\",\"scale\":0}},{\"name\":\"C3_DATE\",\"type\":\"date\",\"nullable\":true,\"metadata\":{\"name\":\"C3_DATE\",\"scale\":0}},{\"name\":\"C4_NUMERIC\",\"type\":\"decimal(15,2)\",\"nullable\":true,\"metadata\":{\"name\":\"C4_DECIMAL\",\"scale\":2}},{\"name\":\"C5_DOUBLE\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"name\":\"C5_DOUBLE\",\"scale\":0}},{\"name\":\"C6_INT\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"name\":\"C6_INT\",\"scale\":0}},{\"name\":\"C7_BIGINT\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"name\":\"C7_BIGINT\",\"scale\":0}},{\"name\":\"C8_FLOAT\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"name\":\"C8_FLOAT\",\"scale\":0}},{\"name\":\"C9_SMALLINT\",\"type\":\"short\",\"nullable\":true,\"metadata\":{\"name\":\"C9_SMALLINT\",\"scale\":0}},{\"name\":\"C10_TIME\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{\"name\":\"C10_TIME\",\"scale\":0}},{\"name\":\"C11_TIMESTAMP\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{\"name\":\"C11_TIMESTAMP\",\"scale\":9}},{\"name\":\"C12_VARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C12_VARCHAR\",\"scale\":0}},{\"name\":\"C13_DECIMAL\",\"type\":\"decimal(4,1)\",\"nullable\":true,\"metadata\":{\"name\":\"C13_DECIMAL\",\"scale\":1}},{\"name\":\"C14_BIGINT\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"name\":\"C14_BIGINT\",\"scale\":0}},{\"name\":\"C15_LONGVARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C15_LONGVARCHAR\",\"scale\":0}},{\"name\":\"C16_REAL\",\"type\":\"float\",\"nullable\":true,\"metadata\":{\"name\":\"C16_REAL\",\"scale\":0}},{\"name\":\"C17_INT\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"name\":\"C17_INT\",\"scale\":0}}]}"
  // SPARK-22002 removed metadata from StructFields in JdbcUtils.getSchema() (since Spark 2.3)
  val schemaWithoutMetadata = """{"type":"struct","fields":[{"name":"C1_BOOLEAN","type":"boolean","nullable":true,"metadata":{}},{"name":"C2_CHAR","type":"string","nullable":true,"metadata":{}},{"name":"C3_DATE","type":"date","nullable":true,"metadata":{}},{"name":"C4_NUMERIC","type":"decimal(15,2)","nullable":true,"metadata":{}},{"name":"C5_DOUBLE","type":"double","nullable":true,"metadata":{}},{"name":"C6_INT","type":"integer","nullable":false,"metadata":{}},{"name":"C7_BIGINT","type":"long","nullable":false,"metadata":{}},{"name":"C8_FLOAT","type":"double","nullable":true,"metadata":{}},{"name":"C9_SMALLINT","type":"short","nullable":true,"metadata":{}},{"name":"C10_TIME","type":"timestamp","nullable":true,"metadata":{}},{"name":"C11_TIMESTAMP","type":"timestamp","nullable":true,"metadata":{}},{"name":"C12_VARCHAR","type":"string","nullable":true,"metadata":{}},{"name":"C13_DECIMAL","type":"decimal(4,1)","nullable":true,"metadata":{}},{"name":"C14_BIGINT","type":"long","nullable":true,"metadata":{}},{"name":"C15_LONGVARCHAR","type":"string","nullable":true,"metadata":{}},{"name":"C16_REAL","type":"float","nullable":true,"metadata":{}},{"name":"C17_INT","type":"integer","nullable":true,"metadata":{}}]}"""

  val checkTheUrl = "java.lang.IllegalArgumentException: requirement failed: The driver could not open a JDBC connection. Check the URL"
  val connectionNotCreated = "Connection not created"
}
