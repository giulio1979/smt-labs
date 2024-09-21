package org.vale.kafka.smts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

public class FlattenJsonTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    // Configuration parameters
    public static final String FLATTEN_ARRAY_FIELDS_CONFIG = "flatten.array.fields";
    public static final String JSON_VALUE_FIELD_CONFIG = "json.value.field";

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private Set<String> fieldsToFlatten;
    private String valueField;

    @Override
    public void configure(Map<String, ?> configs) {
        // Load configurations
        SimpleConfig config = new SimpleConfig(config(), configs);

        // Retrieve configured fields to flatten
        String flattenArrayFields = config.getString(FLATTEN_ARRAY_FIELDS_CONFIG);
        fieldsToFlatten = new HashSet<>(Arrays.asList(flattenArrayFields.split(",")));

        // Retrieve the configured JSON field name
        valueField = config.getString(JSON_VALUE_FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        // Get the value field dynamically
        Object value = record.value();
        if (value instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>) value;
            Object jsonFieldValue = valueMap.get(valueField);

            if (jsonFieldValue instanceof String) {
                try {
                    // Parse the JSON string into a JsonNode
                    JsonNode rootNode = objectMapper.readTree((String) jsonFieldValue);

                    // Create a map to hold the flattened result
                    Map<String, Object> flattenedMap = new HashMap<>();
                    flattenJson("", rootNode, flattenedMap);

                    // Convert the flattened map back to JSON string
                    String transformedJson = objectMapper.writeValueAsString(flattenedMap);

                    // Update the value field with the transformed JSON
                    valueMap.put(valueField, transformedJson);

                    // Return a new record with the transformed value map
                    return record.newRecord(
                            record.topic(),
                            record.kafkaPartition(),
                            record.keySchema(),
                            record.key(),
                            record.valueSchema(),
                            valueMap,
                            record.timestamp()
                    );

                } catch (Exception e) {
                    e.printStackTrace();  // Add proper logging
                }
            }
        }

        // If the record doesn't contain the specified field, return it unchanged
        return record;
    }

    private void flattenJson(String currentPath, JsonNode node, Map<String, Object> result) {
        if (node.isObject()) {
            // Iterate over object fields
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                String newKey = currentPath.isEmpty() ? fieldName : currentPath + capitalize(fieldName);

                // Recursively flatten nested objects
                flattenJson(newKey, fieldValue, result);
            }
        } else if (node.isArray()) {
            // Handle arrays: Check if the current path should be flattened
            if (fieldsToFlatten.contains(currentPath)) {
                flattenArrayWithIndex(currentPath, node, result);
            } else {
                // Keep array as-is
                result.put(currentPath, node);
            }
        } else {
            // Base case: add primitive values
            result.put(currentPath, node.asText());
        }
    }

    private void flattenArrayWithIndex(String currentPath, JsonNode arrayNode, Map<String, Object> result) {
        int index = 0;
        for (JsonNode arrayElement : arrayNode) {
            String indexedKey = currentPath + index;
            if (arrayElement.isObject()) {
                // Flatten objects inside the array
                flattenJson(indexedKey, arrayElement, result);
            } else if (arrayElement.isArray()) {
                // Nested arrays can remain as-is (or handle them recursively if needed)
                result.put(indexedKey, arrayElement);
            } else {
                // Primitive values
                result.put(indexedKey, arrayElement.asText());
            }
            index++;
        }
    }

    private String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    @Override
    public void close() {
        // Cleanup logic if needed
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FLATTEN_ARRAY_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Comma-separated list of JSON paths for arrays to be flattened with index.")
                .define(JSON_VALUE_FIELD_CONFIG, ConfigDef.Type.STRING, "value", ConfigDef.Importance.HIGH,
                        "The field in the record that contains the JSON string to be transformed.");
    }
}
