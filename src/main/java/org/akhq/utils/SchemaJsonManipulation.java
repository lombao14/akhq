package org.akhq.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;

public class SchemaJsonManipulation {

    public static void removeFieldRecursive(JsonNode parentNode, JsonNode currentNode, String fieldName) {
        if (currentNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) currentNode;
            JsonNode field = objectNode.get(fieldName);
            if (field != null) {
                if (parentNode != null) {
                    ((ObjectNode) parentNode).remove(parentNode.fieldNames().next());
                } else {
                    throw new RuntimeException(String.format("The parent node of  [%s] could not be removed", fieldName));
                }
            } else {
                for (JsonNode child : currentNode) {
                    removeFieldRecursive(currentNode, child, fieldName);
                }
            }
        } else if (currentNode.isArray()) {
            for (JsonNode child : currentNode) {
                removeFieldRecursive(currentNode, child, fieldName);
            }
        }
    }

    public static JsonNode schemaToJsonNode(Schema schema) {
        try {
            return new ObjectMapper().readTree(schema.toString());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
