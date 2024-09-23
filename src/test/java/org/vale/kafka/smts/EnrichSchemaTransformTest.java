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
        Schema productsSchema = transformedSchema.field("products").schema().valueSchema();
        assertThat(productsSchema.field("id")).isNotNull();
        assertThat(productsSchema.field("name").schema().isOptional()).isTrue();
        assertThat(productsSchema.field("price").schema().isOptional()).isTrue();

        // Check the actual values in the transformed struct
        List<Struct> products = (List<Struct>) transformedStruct.get("products");
        assertThat(products).hasSize(3);
        assertThat(products.get(0).getString("name")).isEqualTo("Laptop");
        assertThat(products.get(1).getFloat64("price")).isEqualTo(999.99);
    }

    @Test
    public void testJsonArray() throws Exception {
        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("jsonData", Schema.STRING_SCHEMA)
                .build();

        Struct originalStruct = new Struct(originalSchema)
                .put("id", 4)
                .put("jsonData", "{\"items\":[{\"item\":\"Book\"},{\"item\":\"Pen\"}]}");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, originalSchema, originalStruct, 0);
        SinkRecord transformedRecord = transform.apply(record);

        Struct transformedStruct = (Struct) transformedRecord.value();
        Schema transformedSchema = transformedRecord.valueSchema();

        assertThat(transformedSchema.field("items")).isNotNull();
        Schema itemsSchema = transformedSchema.field("items").schema().valueSchema();
        assertThat(itemsSchema.field("item")).isNotNull();
    }


    @Test
    public void testSingleItemInArray() throws Exception {
        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("jsonData", Schema.STRING_SCHEMA)
                .build();

        Struct originalStruct = new Struct(originalSchema)
                .put("id", 6)
                .put("jsonData", "{\"products\":[{\"name\":\"Notebook\"}]}");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, originalSchema, originalStruct, 0);
        SinkRecord transformedRecord = transform.apply(record);

        Struct transformedStruct = (Struct) transformedRecord.value();
        Schema transformedSchema = transformedRecord.valueSchema();

        assertThat(transformedSchema.field("products")).isNotNull();
        Schema productsSchema = transformedSchema.field("products").schema().valueSchema();
        assertThat(productsSchema.field("name")).isNotNull();
    }

    @Test
    public void testArrayWithNullValues() throws Exception {
        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("jsonData", Schema.STRING_SCHEMA)
                .build();

        Struct originalStruct = new Struct(originalSchema)
                .put("id", 7)
                .put("jsonData", "{\"items\":[{\"name\":null},{\"name\":\"Eraser\"}]}");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, originalSchema, originalStruct, 0);
        SinkRecord transformedRecord = transform.apply(record);

        Struct transformedStruct = (Struct) transformedRecord.value();
        Schema transformedSchema = transformedRecord.valueSchema();

        assertThat(transformedSchema.field("items")).isNotNull();
        Schema itemsSchema = transformedSchema.field("items").schema().valueSchema();
        assertThat(itemsSchema.field("name").schema().isOptional()).isTrue();  // Should be optional
    }


    @Test
    public void testNestedArrayAndObject() throws Exception {
        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("jsonData", Schema.STRING_SCHEMA)
                .build();

        Struct originalStruct = new Struct(originalSchema)
                .put("id", 9)
                .put("jsonData", "{\"groups\":[{\"name\":\"Group1\",\"members\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}]}");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, originalSchema, originalStruct, 0);
        SinkRecord transformedRecord = transform.apply(record);

        Struct transformedStruct = (Struct) transformedRecord.value();
        Schema transformedSchema = transformedRecord.valueSchema();

        assertThat(transformedSchema.field("groups")).isNotNull();
        Schema groupsSchema = transformedSchema.field("groups").schema().valueSchema();
        assertThat(groupsSchema.field("name")).isNotNull();
        assertThat(groupsSchema.field("members")).isNotNull();
    }

}
