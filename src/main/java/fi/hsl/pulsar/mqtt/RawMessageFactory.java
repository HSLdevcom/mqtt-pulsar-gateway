package fi.hsl.pulsar.mqtt;

import com.google.protobuf.ByteString;
import fi.hsl.common.mqtt.proto.Mqtt;

import java.util.function.BiFunction;

public class RawMessageFactory implements IMapperFactory {
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
}
