package fi.hsl.pulsar.mqtt;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.transitdata.TransitdataProperties;
import java.net.URL;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiFunction;
import org.junit.Test;

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

    @Test
    public void checkProperties() {
        Map<String, String> props = new RawMessageFactory().properties();
        assertEquals(2, props.size());
        assertEquals(props.get(TransitdataProperties.KEY_SCHEMA_VERSION), "1");
        assertEquals(
                props.get(TransitdataProperties.KEY_PROTOBUF_SCHEMA),
                TransitdataProperties.ProtobufSchema.MqttRawMessage.toString());
    }

    @Test(expected = Exception.class)
    public void validatePropertiesAreImmutable() {
        Map<String, String> props = new RawMessageFactory().properties();
        props.remove(TransitdataProperties.KEY_SCHEMA_VERSION);
    }
}
