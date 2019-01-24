package fi.hsl.pulsar.mqtt.hfp;

import fi.hsl.pulsar.mqtt.IMapperFactory;

import java.util.function.BiFunction;

public class HfpMapperFactory implements IMapperFactory {
    @Override
    public BiFunction<String, byte[], byte[]> createMapper() {
        return (topic, payload) -> {
            //TODO parse into protobuf format
            return payload;
        };
    }
}
