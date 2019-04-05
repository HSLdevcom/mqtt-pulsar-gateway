package fi.hsl.pulsar.mqtt;

import com.google.protobuf.ByteString;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.transitdata.TransitdataProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class RawMessageFactory implements IMapperFactory {

    private final static Map<String, String> properties;
    static {
        HashMap<String, String> props = new HashMap<>();
        props.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.MqttRawMessage.toString());
        props.put(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(Mqtt.RawMessage.newBuilder().getSchemaVersion()));
        properties = Collections.unmodifiableMap(props);
    }

    @Override
    public BiFunction<String, byte[], byte[]> createMapper() {
        return (topic, payload) -> {
            Mqtt.RawMessage.Builder builder = Mqtt.RawMessage.newBuilder();

            Mqtt.RawMessage raw = builder
                    .setSchemaVersion(builder.getSchemaVersion())
                    .setTopic(topic)
                    .setPayload(ByteString.copyFrom(payload))
                    .build();

            return raw.toByteArray();
        };
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }
}
