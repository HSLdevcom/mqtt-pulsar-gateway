package fi.hsl.pulsar.mqtt;

import java.util.function.BiFunction;

public interface IMapperFactory {
    /**
     * Create a mapping function which can be used to create new Pulsar payload of type byte[]
     * from MQTT topic and the MQTT message payload.
     *
     * @return Mapping function
     */
    BiFunction<String, byte[], byte[]> createMapper();
}
