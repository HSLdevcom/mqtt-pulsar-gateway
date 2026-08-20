package fi.hsl.pulsar.mqtt;

import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.pulsar.mqtt.service.RawMessageMapper;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.arbitraries.StringArbitrary;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-based tests for {@link RawMessageMapper}.
 *
 * <p>These complement the example-based test in {@link RawMessageMapperTest}
 * by exploring the full input space of topics and payloads.</p>
 */
class RawMessageMapperPropertyTest {

    private final RawMessageMapper mapper = new RawMessageMapper();

    /**
     * Serializing a topic and payload into a {@link Mqtt.RawMessage} and
     * deserializing the bytes must yield the original topic and payload.
     */
    @Property
    void roundtripPreservesTopicAndPayload(@ForAll("validUtf8Strings") String topic, @ForAll byte[] payload)
            throws Exception {
        byte[] serialized = mapper.toRawMessageBytes(topic, payload);
        Mqtt.RawMessage deserialized = Mqtt.RawMessage.parseFrom(serialized);

        assertEquals(topic, deserialized.getTopic());
        assertArrayEquals(payload, deserialized.getPayload().toByteArray());
    }

    /**
     * The schema version embedded in every serialized message must equal
     * the value returned by {@link RawMessageMapper#schemaVersion()}.
     */
    @Property
    void schemaVersionIsConsistent(@ForAll("validUtf8Strings") String topic, @ForAll byte[] payload) throws Exception {
        byte[] serialized = mapper.toRawMessageBytes(topic, payload);
        Mqtt.RawMessage deserialized = Mqtt.RawMessage.parseFrom(serialized);

        assertEquals(mapper.schemaVersion(), deserialized.getSchemaVersion());
    }

    /**
     * Generates strings that survive a protobuf UTF-8 roundtrip.
     * Excludes surrogate code points ({@code U+D800}–{@code U+DFFF}) and
     * the non-character {@code U+FFFE}/{@code U+FFFF}.
     */
    @Provide
    StringArbitrary validUtf8Strings() {
        return net.jqwik.api.Arbitraries.strings().withCharRange('\u0000', '\uD7FF').withCharRange('\uE000', '\uFFFD');
    }
}
