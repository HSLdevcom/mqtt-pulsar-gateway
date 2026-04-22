package fi.hsl.pulsar.mqtt;

import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.pulsar.mqtt.service.RawMessageMapper;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RawMessageMapperTest {
    @Test
    public void testWithJsonFile() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource("hfp-sample.json");
        String content = new Scanner(url.openStream(), StandardCharsets.UTF_8).useDelimiter("\\A").next();

        final byte[] payload = content.getBytes(StandardCharsets.UTF_8);
        assertNotNull(payload);
        final String topic = "/topic/with/json/payload/#";

        RawMessageMapper mapper = new RawMessageMapper();
        byte[] mapped = mapper.toRawMessageBytes(topic, payload);

        Mqtt.RawMessage msg = Mqtt.RawMessage.parseFrom(mapped);
        assertNotNull(msg);

        assertEquals(topic, msg.getTopic());
        assertArrayEquals(payload, msg.getPayload().toByteArray());
        assertEquals(1, mapper.schemaVersion());
    }
}
