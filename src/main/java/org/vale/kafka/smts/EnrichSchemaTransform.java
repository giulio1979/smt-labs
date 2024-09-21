package org.vale.kafka.smts;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class EnrichSchemaTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_CONFIG = "json_field";
    private static final String PURPOSE = "enrich schema and replace JSON string with object";

    private ObjectMapper objectMapper = new ObjectMapper();
    private String fieldName;
    private Schema currentSchema;


    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Field name containing the JSON string.");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(config(), configs);
        fieldName = config.getString(FIELD_CONFIG);
        currentSchema = null;  // Initially, we don't have a schema
    }

    @Override
    public R apply(R record) {
        Schema valueSchema = record.valueSchema();
        Object value = record.value();

        if (valueSchema != null && value instanceof Struct) {
            Struct struct = (Struct) value;
            if (struct.get(fieldName) instanceof String) {
                try {
                    String jsonString = (String) struct.get(fieldName);
                    Map<String, Object> parsedJson = objectMapper.readValue(jsonString, Map.class);

                    // Dynamically enrich the schema with new fields
                    Schema enrichedSchema = dynamicallyEnrichSchema(parsedJson);

                    // Build new Struct with the enriched schema
                    Struct newStruct = buildStructWithSchema(enrichedSchema, struct, parsedJson);

                    return record.newRecord(
                            record.topic(),
                            record.kafkaPartition(),
                            record.keySchema(),
                            record.key(),
                            enrichedSchema,
                            newStruct,
                            record.timestamp()
                    );
                } catch (Exception e) {
                    throw new RuntimeException("Error parsing JSON in field " + fieldName, e);
                }
            }
        }

        return record;  // No transformation if schema or field isn't present
    }

    private Schema dynamicallyEnrichSchema(Map<String, Object> jsonFields) {
        // Start with the current schema and build upon it
        SchemaBuilder builder = SchemaBuilder.struct();

// If currentSchema exists, add its fields to the builder first
        if (currentSchema != null) {
            for (Field field : currentSchema.fields()) {
                builder.field(field.name(), field.schema());
            }
        }

// Now, add new fields from the parsed JSON object, marking them as optional
        for (Map.Entry<String, Object> entry : jsonFields.entrySet()) {
            // Add the field only if it doesn't already exist in the current schema
            if (currentSchema == null || currentSchema.field(entry.getKey()) == null) {
                Schema fieldSchema = inferSchema(entry.getValue());
                System.out.println(fieldSchema.toString());
                builder.field(entry.getKey(), fieldSchema);
            }
        }

// Build the enriched schema with both old and new fields
        currentSchema = builder.build();
        return currentSchema;
    }

    private Struct buildStructWithSchema(Schema enrichedSchema, Struct originalStruct, Map<String, Object> jsonFields) {
        Struct newStruct = new Struct(enrichedSchema);

        // Copy original fields
        for (Field field : enrichedSchema.fields()) {
            if (field.name().equals(fieldName)) {
                newStruct.put(field, jsonFields);
            } else if (originalStruct.schema().field(field.name()) != null) {
                newStruct.put(field, originalStruct.get(field.name()));
            } else {
                newStruct.put(field, null);  // If the field is not present, we set it to null
            }
        }

        return newStruct;
    }
    private Schema inferSchema(Object value) {
        if (value instanceof Integer) {
            return Schema.INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.INT64_SCHEMA;
        } else if (value instanceof Float) {
            return Schema.FLOAT32_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.FLOAT64_SCHEMA;
        } else if (value instanceof Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        } else if (value instanceof String) {
            return Schema.STRING_SCHEMA;
        } else if (value instanceof Map) {
            // If it's a nested Map, we recursively build a schema for it
            Map<String, Object> map = (Map<String, Object>) value;
            SchemaBuilder structBuilder = SchemaBuilder.struct();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                structBuilder.field(entry.getKey(), inferSchema(entry.getValue()));
            }
            return structBuilder.build();
        } else if (value instanceof List) {
            // If it's an array (List), we merge schemas across all elements
            List<?> list = (List<?>) value;
            if (!list.isEmpty()) {
                // Start with an empty map to accumulate schemas
                Map<String, Schema> mergedSchemas = new HashMap<>();

                for (Object element : list) {
                    if (element instanceof Map) {
                        // Merge the schemas of all map elements within the list
                        Map<String, Object> elementMap = (Map<String, Object>) element;
                        for (Map.Entry<String, Object> entry : elementMap.entrySet()) {
                            Schema inferredFieldSchema = inferSchema(entry.getValue());
                            if (!mergedSchemas.containsKey(entry.getKey())) {
                                // Add new field to the merged schema
                                mergedSchemas.put(entry.getKey(), inferredFieldSchema);
                            } else {
                                // Merge the schema for existing fields, making them optional if needed
                                Schema existingSchema = mergedSchemas.get(entry.getKey());
                                mergedSchemas.put(entry.getKey(), mergeSchemas(existingSchema, inferredFieldSchema));
                            }
                        }
                    }
                }

                // Build the schema for array elements using the merged fields
                SchemaBuilder elementSchemaBuilder = SchemaBuilder.struct();
                for (Map.Entry<String, Schema> fieldSchema : mergedSchemas.entrySet()) {
                    elementSchemaBuilder.field(fieldSchema.getKey(), fieldSchema.getValue());
                }
                Schema merged = elementSchemaBuilder.build();
                return SchemaBuilder.array(merged).optional().build();
            } else {
                // Default schema for empty array
                return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
            }
        }
        return Schema.OPTIONAL_STRING_SCHEMA;  // Default schema for unrecognized types
    }


    private Schema mergeSchemas(Schema schema1, Schema schema2) {
        if (schema1.type() == schema2.type()) {
            return schema1;  // Same type, no need to change
        } else {
            // If types differ, we generalize by making the schema optional, or other logic if needed
            return SchemaBuilder.type(schema1.type()).optional().build();
        }
    }


    @Override
    public void close() {
    }

}

