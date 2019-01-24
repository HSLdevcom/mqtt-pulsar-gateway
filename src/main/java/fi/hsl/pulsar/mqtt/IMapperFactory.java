package fi.hsl.pulsar.mqtt;

import java.util.function.BiFunction;

public interface IMapperFactory {
    /**
     * Create a mapping function which can be used to create new Pulsar payload of type byte[]
     * from MQTT topic and the MQTT message payload.
     *
     * The function needs to be thread-safe, so don't store any state inside the mapper.
     *
     * @return Mapping function
     */
    BiFunction<String, byte[], byte[]> createMapper();
}
