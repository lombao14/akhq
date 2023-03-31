package org.akhq.utils;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.avro.random.generator.Generator;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;

import static org.akhq.utils.SchemaJsonManipulation.removeFieldRecursive;
import static org.akhq.utils.SchemaJsonManipulation.schemaToJsonNode;

public class AvroRandomGenerator {

    public String generateRandomData(Schema schema) throws IOException {
        Schema cleanedSchema = cleanProblematicFields(schema);
        Generator generator = new Generator.Builder().schema(cleanedSchema).build();
        return String.valueOf(generator.generate());
    }

    private static Schema cleanProblematicFields(Schema schema) {
        final JsonNode schemaAsJsonNode = schemaToJsonNode(schema);
        removeFieldRecursive(null, schemaAsJsonNode, "file");
        return new Schema.Parser().parse(schemaAsJsonNode.toString());
    }

}
