package org.vale.kafka.smts;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

                    // Check if the parsed JSON is empty
                    if (parsedJson.isEmpty()) {
                        return record;  // Return the original record if JSON is empty
                    }

                    Schema enrichedSchema = dynamicallyEnrichSchema(parsedJson);

                    // If the schema hasn't changed, return the original record
                    if (enrichedSchema == valueSchema) {
                        return record;
                    }

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

        return record;
    }


    /**
     * Infer schema based on the value type.
     */
    private Schema inferSchema(Object value) {
        if (value == null) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value instanceof Integer) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        } else if (value instanceof Float) {
            return Schema.OPTIONAL_FLOAT32_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else if (value instanceof Boolean) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        } else if (value instanceof String) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value instanceof List) {
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
        } else if (value instanceof Map) {
            SchemaBuilder builder = SchemaBuilder.struct().optional();
            Map<?, ?> map = (Map<?, ?>) value;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() instanceof String) {
                    String key = (String) entry.getKey();
                    Schema fieldSchema = inferSchema(entry.getValue());
                    builder.field(key, fieldSchema);
                }
            }
            return builder.build();
        }
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    private Schema dynamicallyEnrichSchema(Map<String, Object> jsonFields) {
        SchemaBuilder builder = SchemaBuilder.struct();

        if (currentSchema != null) {
            for (Field field : currentSchema.fields()) {
                builder.field(field.name(), field.schema());
            }
        }

        for (Map.Entry<String, Object> entry : jsonFields.entrySet()) {
            if (currentSchema == null || currentSchema.field(entry.getKey()) == null) {
                Schema fieldSchema = inferSchema(entry.getValue());
                builder.field(entry.getKey(), fieldSchema);
            }
        }

        currentSchema = builder.build();
        return currentSchema;
    }

    private Object convertValue(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        switch (schema.type()) {
            case STRUCT:
                Struct struct = new Struct(schema);
                Map<?, ?> map = (Map<?, ?>) value;
                for (Field field : schema.fields()) {
                    Object fieldValue = map.get(field.name());
                    struct.put(field, convertValue(fieldValue, field.schema()));
                }
                return struct;
            case ARRAY:
                List<?> list = (List<?>) value;
                List<Object> convertedList = new ArrayList<>(list.size());
                for (Object item : list) {
                    convertedList.add(convertValue(item, schema.valueSchema()));
                }
                return convertedList;
            default:
                return value;
        }
    }

    private Struct buildStructWithSchema(Schema enrichedSchema, Struct originalStruct, Map<String, Object> jsonFields) {
        Struct newStruct = new Struct(enrichedSchema);

        for (Field field : enrichedSchema.fields()) {
            String fieldName = field.name();
            if (fieldName.equals(this.fieldName)) {
                newStruct.put(field, originalStruct.get(this.fieldName));
            } else if (originalStruct.schema().field(fieldName) != null) {
                newStruct.put(field, originalStruct.get(fieldName));
            } else {
                Object value = jsonFields.get(fieldName);
                newStruct.put(field, convertValue(value, field.schema()));
            }
        }

        return newStruct;
    }


    /**
     * Merge two schemas, generalizing if necessary (e.g., making fields optional).
     */
    private Schema mergeSchemas(Schema schema1, Schema schema2) {
        if (schema1.type() == schema2.type()) {
            return schema1;  // Same type, no need to change
        } else {
            // If types differ, generalize by making the schema optional
            return SchemaBuilder.type(schema1.type()).optional().build();
        }
    }

    @Override
    public void close() {
        // Clean up resources if needed
    }
}
