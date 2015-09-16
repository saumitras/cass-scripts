/*
package bloat

import java.io.FileOutputStream

import scala.pickling.binary.BinaryPickle

class Serializer {
  class Brotherhood(
                     var name: String,
                     var brothers: Option[Seq[Brotherhood]])

  def pickleTo(brothers: Brotherhood, filename: String) = {
    // convert to Pickle binary object
    val brothersPickle = brothers.pickle
    // get the pickle bytes array
    val brothersPickleByteArray = brothersPickle.value
    // write bytes array into the file
    val fos = new FileOutputStream("./" + filename)
    fos.write(brothersPickleByteArray)
    fos.close
  }

  def unpickleFrom(filename: String): Brotherhood = {
    val brothersRawFromFile = scala.io.Source.fromFile("./" + filename).map(_.toByte).toArray
    val brothersUnpickleValue = BinaryPickle(brothersRawFromFile)
    val brothersUnpickle = brothersUnpickleValue.unpickle[Brotherhood]
    brothersUnpickle
  }

  object SerializationDemo extends App {
    val tom = new Brotherhood("Tom", None)
    val smith = new Brotherhood("Smith", None)
    val richard = new Brotherhood("Richard", None)
    val brothers = new Brotherhood("Assassin", Option(Seq(tom, smith, richard)))

    pickleTo(brothers, "PickleBinaryFile")
    val brotherhoodUnpickle = unpickleFrom("PickleBinaryFile")
  }
}


*/
