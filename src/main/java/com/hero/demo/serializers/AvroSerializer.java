package com.hero.demo.serializers;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroSerializer {

    public ByteBuffer getGenericRecord(Map<String, Object> data, String entityName) {
        InputStream schemaInputStream =
                this.getClass().getResourceAsStream("/avro-schemas/" + entityName + ".avsc");
        try {
            Schema schema = new Schema.Parser().parse(schemaInputStream);
            GenericRecord genericRecord = new GenericData.Record(schema);

            for (String key : data.keySet()) {
                genericRecord.put(key, data.get(key));
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(genericRecord, encoder);
            encoder.flush();
            out.close();

            return ByteBuffer.wrap(out.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, Object> getMap(ByteBuffer byteBuffer, String entityName, List<String> attributes) {
        InputStream schemaInputStream =
                this.getClass().getResourceAsStream("/avro-schemas/" + entityName + ".avsc");
        try {
            Schema schema = new Schema.Parser().parse(schemaInputStream);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
            GenericRecord genericRecord = reader.read(null, decoder);
            Map<String, Object> result = new HashMap<>();
            for (String attribute : attributes) {
                result.put(attribute, genericRecord.get(attribute));
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
