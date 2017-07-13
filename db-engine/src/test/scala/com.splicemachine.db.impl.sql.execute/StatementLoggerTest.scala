import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}

import com.splicemachine.spark.splicemachine.{TestContext}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SplicemachineContextTest extends FunSuite with TestContext with Matchers {
  val rowCount = 10

  private def serialize(value: Any): Array[Byte] = {


  private def deserialize(bytes: Array[Byte]): Any = {

  }

  test("Test SplicemachineContext serialization") {

  }
