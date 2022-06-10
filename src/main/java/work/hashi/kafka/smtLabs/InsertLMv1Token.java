package work.hashi.kafka.smtLabs;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.logicmonitor.auth.LMv1TokenGenerator;

import java.time.Instant;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class InsertLMv1Token<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Add LM v1 Token into header for authorization.";
    public static final String ACCESS_KEY = "access.key";
    public static final String ACCESS_ID = "access.id";
    public static final String AUTH_HEADER_FIELD = "Authorization";
    public static final String HTTP_VERB = "POST";
    public static final String RESOURCE_PATH = "/log/ingest";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ACCESS_KEY, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "Logic Monitor Access Key")
            .define(ACCESS_ID, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "Logic Monitor Access ID");

    private String accessKey;
    private String accessId;

    @Override
    public R apply(R record) {
        Headers updatedHeaders = record.headers().duplicate();
        updatedHeaders.add(AUTH_HEADER_FIELD, Values.parseString(generateLMv1Token(record.value())));
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() { }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        accessKey = config.getString(ACCESS_KEY);
        accessId = config.getString(ACCESS_ID);
    }

    private String generateLMv1Token(Object fieldMessage) {
        long epochTime = Instant.now().toEpochMilli();
        return LMv1TokenGenerator.generate(accessId, accessKey, HTTP_VERB,
                String.valueOf(fieldMessage), RESOURCE_PATH, epochTime);
    }
}
