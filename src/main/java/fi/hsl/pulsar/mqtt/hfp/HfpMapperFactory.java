package fi.hsl.pulsar.mqtt.hfp;

import fi.hsl.pulsar.mqtt.IMapperFactory;

import java.util.Optional;
import java.util.function.BiFunction;

public class HfpMapperFactory implements IMapperFactory {
    @Override
    public BiFunction<String, byte[], byte[]> createMapper() {
        final MessageParser parser = MessageParser.newInstance();

        return (topic, payload) -> {
            Optional<HfpMessage> maybeMessage = parser.safeParse(payload);
            Optional<HfpMetadata> maybeMetadata = MessageParser.safeParseMetadata(topic);

            Optional<byte[]> formatted = maybeMessage.flatMap(msg -> maybeMetadata.map(metadata -> {
                    return payload;
                }));
            //TODO map to protobuf format
            return formatted.orElse(null);
        };
    }
}
