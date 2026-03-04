package fi.hsl.pulsar.mqtt.service;

import com.google.protobuf.ByteString;
import fi.hsl.common.mqtt.proto.Mqtt;
import org.springframework.stereotype.Component;

@Component
public class RawMessageMapper {

    public int schemaVersion() {
        return Mqtt.RawMessage.newBuilder().getSchemaVersion();
    }

    public byte[] toRawMessageBytes(String topic, byte[] payload) {
        Mqtt.RawMessage.Builder builder = Mqtt.RawMessage.newBuilder();

        Mqtt.RawMessage raw = builder.setSchemaVersion(builder.getSchemaVersion()).setTopic(topic)
                .setPayload(ByteString.copyFrom(payload)).build();

        return raw.toByteArray();
    }
}
