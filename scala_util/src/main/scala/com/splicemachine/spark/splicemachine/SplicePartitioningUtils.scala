package com.splicemachine.spark.splicemachine

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.lang.{Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util.{Locale, TimeZone}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Resolver, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.types._

// TODO: We should tighten up visibility of the classes here once we clean up Hive coupling.

object PartitionPath {
  def apply(values: InternalRow, path: String): PartitionPath =
    apply(values, new Path(path))
}

/**
 * Holds a directory in a partitioned collection of files as well as the partition values
 * in the form of a Row.  Before scanning, the files at `path` need to be enumerated.
 */
case class PartitionPath(values: InternalRow, path: Path)

case class PartitionSpec(
                          partitionColumns: StructType,
                          partitions: Seq[PartitionPath])

object PartitionSpec {
  val emptySpec = PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[PartitionPath])
}

object SplicePartitioningUtils {

  case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal])
  {
    require(columnNames.size == literals.size)
  }

  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.DEFAULT_PARTITION_NAME
  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName

  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * For example, given:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14
   *   hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28
   * }}}
   * it returns:
   * {{{
   *   PartitionSpec(
   *     partitionColumns = StructType(
   *       StructField(name = "a", dataType = IntegerType, nullable = true),
   *       StructField(name = "b", dataType = StringType, nullable = true),
   *       StructField(name = "c", dataType = DoubleType, nullable = true)),
   *     partitions = Seq(
   *       Partition(
   *         values = Row(1, "hello", 3.14),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14"),
   *       Partition(
   *         values = Row(2, "world", 6.28),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28")))
   * }}}
   */
  def parsePartitions(
                                            paths: Seq[Path],
                                            typeInference: Boolean,
                                            basePaths: Set[Path],
                                            userSpecifiedSchema: Option[StructType],
                                            caseSensitive: Boolean,
                                            timeZoneId: String): PartitionSpec = {
    parsePartitions(paths, typeInference, basePaths, userSpecifiedSchema,
      caseSensitive, DateTimeUtils.getTimeZone(timeZoneId))
  }

  def parsePartitions(
                                            paths: Seq[Path],
                                            typeInference: Boolean,
                                            basePaths: Set[Path],
                                            userSpecifiedSchema: Option[StructType],
                                            caseSensitive: Boolean,
                                            timeZone: TimeZone): PartitionSpec = {
    val userSpecifiedDataTypes = if (userSpecifiedSchema.isDefined) {
      val nameToDataType = userSpecifiedSchema.get.fields.map(f => f.name -> f.dataType).toMap
      if (!caseSensitive) {
        CaseInsensitiveMap(nameToDataType)
      } else {
        nameToDataType
      }
    } else {
      Map.empty[String, DataType]
    }

    // SPARK-26990: use user specified field names if case insensitive.
    val userSpecifiedNames = if (userSpecifiedSchema.isDefined && !caseSensitive) {
      CaseInsensitiveMap(userSpecifiedSchema.get.fields.map(f => f.name -> f.name).toMap)
    } else {
      Map.empty[String, String]
    }

    // First, we need to parse every partition's path and see if we can find partition values.
    val (partitionValues, optDiscoveredBasePaths) = paths.map { path =>
      parsePartition(path, typeInference, basePaths, userSpecifiedDataTypes, timeZone)
    }.unzip

    // We create pairs of (path -> path's partition value) here
    // If the corresponding partition value is None, the pair will be skipped
    val pathsWithPartitionValues = paths.zip(partitionValues).flatMap(x => x._2.map(x._1 -> _))

    if (pathsWithPartitionValues.isEmpty) {
      // This dataset is not partitioned.
      PartitionSpec.emptySpec
    } else {
      // This dataset is partitioned. We need to check whether all partitions have the same
      // partition columns and resolve potential type conflicts.

      // Check if there is conflicting directory structure.
      // For the paths such as:
      // var paths = Seq(
      //   "hdfs://host:9000/invalidPath",
      //   "hdfs://host:9000/path/a=10/b=20",
      //   "hdfs://host:9000/path/a=10.5/b=hello")
      // It will be recognised as conflicting directory structure:
      //   "hdfs://host:9000/invalidPath"
      //   "hdfs://host:9000/path"
      // TODO: Selective case sensitivity.
      val discoveredBasePaths = optDiscoveredBasePaths.flatten.map(_.toString.toLowerCase())
      assert(
        discoveredBasePaths.distinct.size == 1,
        "Conflicting directory structures detected. Suspicious paths:\b" +
          discoveredBasePaths.distinct.mkString("\n\t", "\n\t", "\n\n") +
          "If provided paths are partition directories, please set " +
          "\"basePath\" in the options of the data source to specify the " +
          "root directory of the table. If there are multiple root directories, " +
          "please load them separately and then union them.")

      val resolvedPartitionValues = resolvePartitions(pathsWithPartitionValues, timeZone)

      // Creates the StructType which represents the partition columns.
      val fields = {
        val PartitionValues(columnNames, literals) = resolvedPartitionValues.head
        columnNames.zip(literals).map { case (name, Literal(_, dataType)) =>
          // We always assume partition columns are nullable since we've no idea whether null values
          // will be appended in the future.
          val resultName = userSpecifiedNames.getOrElse(name, name)
          val resultDataType = userSpecifiedDataTypes.getOrElse(name, dataType)
          StructField(resultName, resultDataType, nullable = true)
        }
      }

      // Finally, we create `Partition`s based on paths and resolved partition values.
      val partitions = resolvedPartitionValues.zip(pathsWithPartitionValues).map {
        case (PartitionValues(_, literals), (path, _)) =>
          PartitionPath(InternalRow.fromSeq(literals.map(_.value)), path)
      }

      PartitionSpec(StructType(fields), partitions)
    }
  }

  /**
   * Parses a single partition, returns column names and values of each partition column, also
   * the path when we stop partition discovery.  For example, given:
   * {{{
   *   path = hdfs://<host>:<port>/path/to/partition/a=42/b=hello/c=3.14
   * }}}
   * it returns the partition:
   * {{{
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, DoubleType)))
   * }}}
   * and the path when we stop the discovery is:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition
   * }}}
   */
  def parsePartition(
                                           path: Path,
                                           typeInference: Boolean,
                                           basePaths: Set[Path],
                                           userSpecifiedDataTypes: Map[String, DataType],
                                           timeZone: TimeZone): (Option[PartitionValues], Option[Path]) = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    // currentPath is the current path that we will use to parse partition column value.
    var currentPath: Path = path

    while (!finished) {
      // Sometimes (e.g., when speculative task is enabled), temporary directories may be left
      // uncleaned. Here we simply ignore them.
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        return (None, None)
      }

      if (basePaths.contains(currentPath)) {
        // If the currentPath is one of base paths. We should stop.
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
        parsePartitionColumn(currentPath.getName, typeInference, userSpecifiedDataTypes, timeZone)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop:
        //  - In this iteration, we could not parse the value of partition column and value,
        //    i.e. maybeColumn is None, and columns is not empty. At here we check if columns is
        //    empty to handle cases like /table/a=1/_temporary/something (we need to find a=1 in
        //    this case).
        //  - After we get the new currentPath, this new currentPath represent the top level dir
        //    i.e. currentPath.getParent == null. For the example of "/table/a=1/",
        //    the top level dir is "/table".
        finished =
          (maybeColumn.isEmpty && !columns.isEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames, values)), Some(currentPath))
    }
  }

  private def parsePartitionColumn(
                                    columnSpec: String,
                                    typeInference: Boolean,
                                    userSpecifiedDataTypes: Map[String, DataType],
                                    timeZone: TimeZone): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = if (userSpecifiedDataTypes.contains(columnName)) {
        // SPARK-26188: if user provides corresponding column schema, get the column value without
        //              inference, and then cast it as user specified data type.
        val columnValue = inferPartitionColumnValue(rawColumnValue, false, timeZone)
        val userType = userSpecifiedDataTypes(columnName);

        // modified martinrupp {
        val castedValue =
          Cast(columnValue, userType, Option(timeZone.getID)).eval()

        if(castedValue == null && rawColumnValue != "__HIVE_DEFAULT_PARTITION__") {
          throw new IllegalArgumentException("Column " + columnName + " is " + userType.simpleString + ", can't parse " + rawColumnValue)
        }
        else {
          Literal.create(castedValue, userSpecifiedDataTypes(columnName))
        }
        // modified martinrupp }
      } else {
        inferPartitionColumnValue(rawColumnValue, typeInference, timeZone)
      }
      Some(columnName -> literal)
    }
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types using
   * [[findWiderTypeForPartitionColumn]].
   */
  def resolvePartitions(
                         pathsWithPartitionValues: Seq[(Path, PartitionValues)],
                         timeZone: TimeZone): Seq[PartitionValues] = {
    if (pathsWithPartitionValues.isEmpty) {
      Seq.empty
    } else {
      // TODO: Selective case sensitivity.
      val distinctPartColNames =
        pathsWithPartitionValues.map(_._2.columnNames.map(_.toLowerCase())).distinct
      assert(
        distinctPartColNames.size == 1,
        listConflictingPartitionColumns(pathsWithPartitionValues))

      // Resolves possible type conflicts for each column
      val values = pathsWithPartitionValues.map(_._2)
      val columnCount = values.head.columnNames.size
      val resolvedValues = (0 until columnCount).map { i =>
        resolveTypeConflicts(values.map(_.literals(i)), timeZone)
      }

      // Fills resolved literals back to each partition
      values.zipWithIndex.map { case (d, index) =>
        d.copy(literals = resolvedValues.map(_(index)))
      }
    }
  }

  def listConflictingPartitionColumns(
                                                            pathWithPartitionValues: Seq[(Path, PartitionValues)]): String = {
    val distinctPartColNames = pathWithPartitionValues.map(_._2.columnNames).distinct

    def groupByKey[K, V](seq: Seq[(K, V)]): Map[K, Iterable[V]] =
      seq.groupBy { case (key, _) => key }.mapValues(_.map { case (_, value) => value })

    val partColNamesToPaths = groupByKey(pathWithPartitionValues.map {
      case (path, partValues) => partValues.columnNames -> path
    })

    val distinctPartColLists = distinctPartColNames.map(_.mkString(", ")).zipWithIndex.map {
      case (names, index) =>
        s"Partition column name list #$index: $names"
    }

    // Lists out those non-leaf partition directories that also contain files
    val suspiciousPaths = distinctPartColNames.sortBy(_.length).flatMap(partColNamesToPaths)

    s"Conflicting partition column names detected:\n" +
      distinctPartColLists.mkString("\n\t", "\n\t", "\n\n") +
      "For partitioned table directories, data files should only live in leaf directories.\n" +
      "And directories at the same level should have the same partition column name.\n" +
      "Please check the following directories for unexpected files or " +
      "inconsistent partition column names:\n" +
      suspiciousPaths.map("\t" + _).mkString("\n", "\n", "")
  }

  // scalastyle:off line.size.limit
  /**
   * Converts a string to a [[Literal]] with automatic type inference. Currently only supports
   * [[NullType]], [[IntegerType]], [[LongType]], [[DoubleType]], [[DecimalType]], [[DateType]]
   * [[TimestampType]], and [[StringType]].
   *
   * When resolving conflicts, it follows the table below:
   *
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * | InputA \ InputB    | NullType          | IntegerType       | LongType          | DecimalType(38,0)* | DoubleType | DateType      | TimestampType | StringType |
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * | NullType           | NullType          | IntegerType       | LongType          | DecimalType(38,0)  | DoubleType | DateType      | TimestampType | StringType |
   * | IntegerType        | IntegerType       | IntegerType       | LongType          | DecimalType(38,0)  | DoubleType | StringType    | StringType    | StringType |
   * | LongType           | LongType          | LongType          | LongType          | DecimalType(38,0)  | StringType | StringType    | StringType    | StringType |
   * | DecimalType(38,0)* | DecimalType(38,0) | DecimalType(38,0) | DecimalType(38,0) | DecimalType(38,0)  | StringType | StringType    | StringType    | StringType |
   * | DoubleType         | DoubleType        | DoubleType        | StringType        | StringType         | DoubleType | StringType    | StringType    | StringType |
   * | DateType           | DateType          | StringType        | StringType        | StringType         | StringType | DateType      | TimestampType | StringType |
   * | TimestampType      | TimestampType     | StringType        | StringType        | StringType         | StringType | TimestampType | TimestampType | StringType |
   * | StringType         | StringType        | StringType        | StringType        | StringType         | StringType | StringType    | StringType    | StringType |
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * Note that, for DecimalType(38,0)*, the table above intentionally does not cover all other
   * combinations of scales and precisions because currently we only infer decimal type like
   * `BigInteger`/`BigInt`. For example, 1.1 is inferred as double type.
   */
  // scalastyle:on line.size.limit
  def inferPartitionColumnValue(
                                                      raw: String,
                                                      typeInference: Boolean,
                                                      timeZone: TimeZone): Literal = {
    val decimalTry = Try {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new JBigDecimal(raw)
      // It reduces the cases for decimals by disallowing values having scale (eg. `1.1`).
      require(bigDecimal.scale <= 0)
      // `DecimalType` conversion can fail when
      //   1. The precision is bigger than 38.
      //   2. scale is bigger than precision.
      Literal(bigDecimal)
    }

    val dateTry = Try {
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // DateType
      DateTimeUtils.getThreadLocalDateFormat(DateTimeUtils.defaultTimeZone()).parse(raw)
      // SPARK-23436: Casting the string to date may still return null if a bad Date is provided.
      // This can happen since DateFormat.parse  may not use the entire text of the given string:
      // so if there are extra-characters after the date, it returns correctly.
      // We need to check that we can cast the raw string since we later can use Cast to get
      // the partition values with the right DataType (see
      // org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex.inferPartitioning)
      val dateValue = Cast(Literal(raw), DateType).eval()
      // Disallow DateType if the cast returned null
      require(dateValue != null)
      Literal.create(dateValue, DateType)
    }

    val timestampTry = Try {
      val unescapedRaw = unescapePathName(raw)
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // TimestampType
      DateTimeUtils.getThreadLocalTimestampFormat(timeZone).parse(unescapedRaw)
      // SPARK-23436: see comment for date
      val timestampValue = Cast(Literal(unescapedRaw), TimestampType, Some(timeZone.getID)).eval()
      // Disallow TimestampType if the cast returned null
      require(timestampValue != null)
      Literal.create(timestampValue, TimestampType)
    }

    if (typeInference) {
      // First tries integral types
      Try(Literal.create(Integer.parseInt(raw), IntegerType))
        .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
        .orElse(decimalTry)
        // Then falls back to fractional types
        .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
        // Then falls back to date/timestamp types
        .orElse(timestampTry)
        .orElse(dateTry)
        // Then falls back to string
        .getOrElse {
          if (raw == DEFAULT_PARTITION_NAME) {
            Literal.create(null, NullType)
          } else {
            Literal.create(unescapePathName(raw), StringType)
          }
        }
    } else {
      if (raw == DEFAULT_PARTITION_NAME) {
        Literal.create(null, NullType)
      } else {
        Literal.create(unescapePathName(raw), StringType)
      }
    }
  }

  private def columnNameEquality(caseSensitive: Boolean): (String, String) => Boolean = {
    if (caseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
  }

  /**
   * Given a collection of [[Literal]]s, resolves possible type conflicts by
   * [[findWiderTypeForPartitionColumn]].
   */
  private def resolveTypeConflicts(literals: Seq[Literal], timeZone: TimeZone): Seq[Literal] = {
    val litTypes = literals.map(_.dataType)
    val desiredType = litTypes.reduce(findWiderTypeForPartitionColumn)

    literals.map { case l @ Literal(_, dataType) =>
      Literal.create(Cast(l, desiredType, Some(timeZone.getID)).eval(), desiredType)
    }
  }

  /**
   * Type widening rule for partition column types. It is similar to
   * [[TypeCoercion.findWiderTypeForTwo]] but the main difference is that here we disallow
   * precision loss when widening double/long and decimal, and fall back to string.
   */
  private val findWiderTypeForPartitionColumn: (DataType, DataType) => DataType = {
    case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) => StringType
    case (DoubleType, LongType) | (LongType, DoubleType) => StringType
    case (t1, t2) => TypeCoercion.findWiderTypeForTwo(t1, t2).getOrElse(StringType)
  }
}
