package fi.hsl.pulsar.mqtt;

import fi.hsl.common.mqtt.proto.Mqtt;
import org.junit.Test;

import java.net.URL;
import java.util.Scanner;
import java.util.function.BiFunction;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RawMessageFactoryTest {
    @Test
    public void testWithJsonFile() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource("hfp-sample.json");
        String content = new Scanner(url.openStream(), "UTF-8").useDelimiter("\\A").next();

        final byte[] payload = content.getBytes("UTF-8");
        assertNotNull(payload);
        final String topic = "/topic/with/json/payload/#";

        BiFunction<String, byte[], byte[]> mapper = new RawMessageFactory().createMapper();
        byte[] mapped = mapper.apply(topic, payload);

        Mqtt.RawMessage msg = Mqtt.RawMessage.parseFrom(mapped);
        assertNotNull(msg);

        assertEquals(topic, msg.getTopic());
        assertArrayEquals(payload, msg.getPayload().toByteArray());

    }
}
