package org.vale.kafka.smts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FlattenJsonTransformTest {

    private FlattenJsonTransform<SinkRecord> transform;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() {
        transform = new FlattenJsonTransform<>();
        Map<String, String> configs = new HashMap<>();
        configs.put(FlattenJsonTransform.FLATTEN_ARRAY_FIELDS_CONFIG, "orders");
        configs.put(FlattenJsonTransform.JSON_VALUE_FIELD_CONFIG, "payload");  // Configure the field containing JSON
        transform.configure(configs);
    }

    @Test
    public void testTransform() throws Exception {
        // Create the input JSON with nested objects and an array
        String jsonPayload = "{\n" +
                "  \"user\": {\n" +
                "    \"firstName\": \"John\",\n" +
                "    \"lastName\": \"Doe\",\n" +
                "    \"address\": {\n" +
                "      \"street\": \"123 Main St\",\n" +
                "      \"city\": \"Anytown\",\n" +
                "      \"details\": {\n" +
                "        \"zipcode\": \"12345\",\n" +
                "        \"state\": \"CA\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"orders\": [\n" +
                "    {\"id\": \"1\", \"item\": {\"name\": \"Book\", \"price\": \"10\"}},\n" +
                "    {\"id\": \"2\", \"item\": {\"name\": \"Pen\", \"price\": \"2\"}}\n" +
                "  ]\n" +
                "}";

        // Mock Kafka record with payload field
        Map<String, Object> value = new HashMap<>();
        value.put("metadata", Map.of("id", "12345", "timestamp", "2023-01-01T12:34:56"));
        value.put("payload", jsonPayload);  // The field that will be transformed

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA), value, 0);

        // Apply the transform
        SinkRecord transformedRecord = transform.apply(record);

        // Validate the result
        Map<String, Object> transformedValue = (Map<String, Object>) transformedRecord.value();
        String transformedJson = (String) transformedValue.get("payload");

        // Deserialize the transformed JSON
        Map<String, Object> result = objectMapper.readValue(transformedJson, Map.class);

        // Assert that fields have been correctly flattened
        assertThat(result)
                .containsEntry("userFirstName", "John")
                .containsEntry("userLastName", "Doe")
                .containsEntry("userAddressStreet", "123 Main St")
                .containsEntry("userAddressCity", "Anytown")
                .containsEntry("userAddressDetailsZipcode", "12345")
                .containsEntry("userAddressDetailsState", "CA")
                .containsEntry("orders0Id", "1")
                .containsEntry("orders0ItemName", "Book")
                .containsEntry("orders0ItemPrice", "10")
                .containsEntry("orders1Id", "2")
                .containsEntry("orders1ItemName", "Pen")
                .containsEntry("orders1ItemPrice", "2");

        // Ensure original record fields are still present
        assertThat(transformedValue).containsKey("metadata");
    }

    @Test
    public void testNoFlattenForUnconfiguredArray() throws Exception {
        // Create JSON without array flattening for unconfigured field
        String jsonPayload = "{\n" +
                "  \"products\": [\n" +
                "    {\"id\": \"1\", \"name\": \"Laptop\"},\n" +
                "    {\"id\": \"2\", \"name\": \"Tablet\"}\n" +
                "  ]\n" +
                "}";

        Map<String, Object> value = new HashMap<>();
        value.put("payload", jsonPayload);

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA), value, 0);

        SinkRecord transformedRecord = transform.apply(record);

        Map<String, Object> transformedValue = (Map<String, Object>) transformedRecord.value();
        String transformedJson = (String) transformedValue.get("payload");

        // Deserialize the transformed JSON
        Map<String, Object> result = objectMapper.readValue(transformedJson, Map.class);

        // Get the expected products JSON as a string for comparison
        String expectedProductsJson = "[{\"id\":\"1\",\"name\":\"Laptop\"},{\"id\":\"2\",\"name\":\"Tablet\"}]";

        // Compare serialized versions of the 'products' field
        String actualProductsJson = objectMapper.writeValueAsString(result.get("products"));

        // Assert that the 'products' array is not flattened and matches the expected value
        assertThat(actualProductsJson).isEqualTo(expectedProductsJson);
    }

}
