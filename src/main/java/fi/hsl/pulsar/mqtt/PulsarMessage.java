package fi.hsl.pulsar.mqtt;

import java.util.Map;

public record PulsarMessage(byte[] payload, long eventTimeMs, Map<String, String> properties) {
}
