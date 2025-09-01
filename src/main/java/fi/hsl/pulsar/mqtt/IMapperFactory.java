package fi.hsl.pulsar.mqtt;

import java.util.Map;
import java.util.function.BiFunction;

public interface IMapperFactory {
    /**
     * Create a mapping function which can be used to create new Pulsar payload of type byte[]
     * from MQTT topic and the MQTT message payload.
     *
     * @return Mapping function
     */
    BiFunction<String, byte[], byte[]> createMapper();

    /**
     * Currently we're assuming all messages created with this one mapper will use the same
     * Pulsar message properties. If this is not the case we need to refactor the above method to
     * return the properties as well.
     *
     * @return Key-value pairs for all Pulsar Message Properties.
     */
    Map<String, String> properties();
}
