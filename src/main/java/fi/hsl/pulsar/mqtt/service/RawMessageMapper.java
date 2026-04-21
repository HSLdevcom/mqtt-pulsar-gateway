package fi.hsl.pulsar.mqtt.service;

import com.google.protobuf.ByteString;
import fi.hsl.common.mqtt.proto.Mqtt;
import org.springframework.stereotype.Component;

@Component
public class RawMessageMapper {

    private static final int SCHEMA_VERSION = Mqtt.RawMessage.getDefaultInstance().getSchemaVersion();

    public int schemaVersion() {
        return SCHEMA_VERSION;
    }

    public byte[] toRawMessageBytes(String topic, byte[] payload) {
        return Mqtt.RawMessage.newBuilder().setSchemaVersion(SCHEMA_VERSION).setTopic(topic)
                .setPayload(ByteString.copyFrom(payload)).build().toByteArray();
    }
}
