import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object AvroSer  extends Serializable  {


  def encode(schemaString : String , row: Row): Array[Byte] = {

    val schema = new Schema.Parser().parse(schemaString)
    val gr: GenericRecord = new GenericData.Record(schema)
    row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(gr, encoder)
    encoder.flush()
    out.close()

    out.toByteArray()
  }


  def encodeUDF(schemaString:String) = udf((x:Row) =>  encode(schemaString, x))
}
