package org.vale.kafka.smts;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class EnrichSchemaTransformTest {

    private EnrichSchemaTransform<SinkRecord> transform;

    @BeforeEach
    public void setup() {
        transform = new EnrichSchemaTransform<>();
        Map<String, String> configs = new HashMap<>();
        configs.put(EnrichSchemaTransform.FIELD_CONFIG, "jsonData");
        transform.configure(configs);
    }

    @Test
    public void testDynamicSchemaEvolutionWithNewField() throws Exception {
        // Create the first record with schema and a JSON string
        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("jsonData", Schema.STRING_SCHEMA)
                .build();

        Struct originalStruct = new Struct(originalSchema)
                .put("id", 1)
                .put("jsonData", "{\"name\":\"John Doe\", \"age\":30}");

        SinkRecord firstRecord = new SinkRecord("test-topic", 0, null, null, originalSchema, originalStruct, 0);
        SinkRecord transformedFirstRecord = transform.apply(firstRecord);

        Struct transformedFirstStruct = (Struct) transformedFirstRecord.value();
        Schema firstTransformedSchema = transformedFirstRecord.valueSchema();

        // Assert schema contains name and age
        assertThat(firstTransformedSchema.field("name")).isNotNull();
        assertThat(firstTransformedSchema.field("age")).isNotNull();

        // Now simulate
    }
    @Test
    public void testInferSchemaFromArrayWithDifferentFields() throws Exception {
        // Create the original schema and struct
        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("jsonData", Schema.STRING_SCHEMA)
                .build();

        // Array with different fields across objects
        Struct originalStruct = new Struct(originalSchema)
                .put("id", 1)
                .put("jsonData", "{\"products\":[{\"id\":\"1\", \"name\":\"Laptop\"}, {\"id\":\"2\", \"price\":999.99}, {\"id\":\"3\", \"name\":\"Tablet\", \"price\":499.99}]}");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, originalSchema, originalStruct, 0);

        // Apply the transform
        SinkRecord transformedRecord = transform.apply(record);

        Struct transformedStruct = (Struct) transformedRecord.value();
        Schema transformedSchema = transformedRecord.valueSchema();

        // Assert schema is enriched with new fields, including merged fields from the array
        assertThat(transformedSchema.field("products")).isNotNull();
        System.out.println(transformedSchema.toString());
        // Now get the schema for the array elements
        Schema productsSchema = transformedSchema.field("products").schema().valueSchema();
        assertThat(productsSchema.field("id")).isNotNull();
        assertThat(productsSchema.field("name").schema().isOptional()).isTrue();  // Optional since "name" is missing in one element
        assertThat(productsSchema.field("price").schema().isOptional()).isTrue();  // Optional since "price" is missing in one element

        // Check the actual values in the transformed struct
        List<Struct> products = (List<Struct>) transformedStruct.get("products");
        assertThat(products).hasSize(3);
        assertThat(products.get(0).getString("name")).isEqualTo("Laptop");
        assertThat(products.get(1).getFloat64("price")).isEqualTo(999.99);
    }
}