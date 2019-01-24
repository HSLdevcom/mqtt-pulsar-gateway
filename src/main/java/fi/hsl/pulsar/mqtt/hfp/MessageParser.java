package fi.hsl.pulsar.mqtt.hfp;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.regex.Pattern;

public class MessageParser {
    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);

    static final Pattern topicVersionRegex = Pattern.compile("(^v\\d+|dev)");

    // Let's use dsl-json (https://github.com/ngs-doo/dsl-json) for performance.
    // Based on this benchmark: https://github.com/fabienrenaud/java-json-benchmark

    //Example: https://github.com/ngs-doo/dsl-json/blob/master/examples/MavenJava8/src/main/java/com/dslplatform/maven/Example.java

    //Note! Apparently not thread safe, for per thread reuse use ThreadLocal pattern or create separate instances
    final DslJson<Object> dslJson = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());

    public static MessageParser newInstance() {
        return new MessageParser();
    }

    public HfpMessage parse(byte[] data) throws IOException {
        return dslJson.deserialize(HfpMessage.class, data, data.length);
    }

    public Optional<HfpMessage> safeParse(byte[] data) {
        try {
            return Optional.ofNullable(parse(data));
        }
        catch (Exception e) {
            log.error("Failed to parse message {}", new String(data));
            return Optional.empty();
        }
    }

    public static Optional<HfpMetadata> safeParseMetadata(String topic) {
        try {
            return parseMetadata(topic);
        }
        catch (Exception e) {
            log.error("Failed to parse message metadata from topic " + topic, e);
            return Optional.empty();
        }
    }

    public static Optional<HfpMetadata> parseMetadata(String topic) throws Exception {
        return parseMetadata(topic, OffsetDateTime.now());
    }

    public static Optional<HfpMetadata> parseMetadata(String topic, OffsetDateTime receivedAt) throws Exception {
        //log.debug("Parsing metadata from topic: " + topic);

        final String[] parts = topic.split("/", -1);//-1 to include empty substrings

        final HfpMetadata meta = new HfpMetadata();
        meta.received_at = receivedAt;
        //We first find the index of version. The prefix topic part can consist of more complicated path
        int versionIndex = findVersionIndex(parts);
        if (versionIndex < 0) {
            log.error("Failed to find topic version from topic " + topic);
            return Optional.empty();
        }


        meta.topic_prefix = joinFirstNParts(parts, versionIndex, "/");
        int index = versionIndex;
        meta.topic_version = parts[index++];

        meta.journey_type = HfpMetadata.JourneyType.valueOf(parts[index++]);
        meta.is_ongoing = "ongoing".equals(parts[index++]);
        meta.mode = HfpMetadata.TransportMode.fromString(parts[index++]);
        meta.owner_operator_id = Integer.parseInt(parts[index++]);
        meta.vehicle_number = Integer.parseInt(parts[index++]);
        meta.unique_vehicle_id = createUniqueVehicleId(meta.owner_operator_id, meta.vehicle_number);
        if (index + 6 <= parts.length) {
            meta.route_id = Optional.ofNullable(validateString(parts[index++]));
            meta.direction_id = Optional.ofNullable(safeParseInt(parts[index++]));
            meta.headsign = Optional.ofNullable(validateString(parts[index++]));
            meta.journey_start_time = Optional.ofNullable(safeParseLocalTime(parts[index++]));
            meta.next_stop_id = Optional.ofNullable(validateString(parts[index++]));
            meta.geohash_level = Optional.ofNullable(safeParseInt(parts[index++]));
        }
        else {
            log.warn("could not parse first batch of additional fields for topic {}", topic);
        }
        if (index + 4 <= parts.length) {
            Optional<GeoHash> maybeGeoHash = parseGeoHash(parts, index);
            meta.topic_latitude = maybeGeoHash.map(hash -> hash.latitude);
            meta.topic_longitude = maybeGeoHash.map(hash -> hash.longitude);
        }
        else {
            log.debug("could not parse second batch of additional fields (geohash) for topic {}", topic);
        }
        return Optional.of(meta);
    }

    public static class GeoHash {
        public double latitude;
        public double longitude;
    }

    static Optional<GeoHash> parseGeoHash(String[] parts, int startIndex) {
        Optional<GeoHash> maybeGeoHash = Optional.empty();

        int index = startIndex;
        final String firstLatLong = parts[index++];
        if (!firstLatLong.isEmpty()) {
            String[] latLong0 = firstLatLong.split(";");
            if (latLong0.length == 2) {
                StringBuffer latitude = new StringBuffer(latLong0[0]).append(".");
                StringBuffer longitude = new StringBuffer(latLong0[1]).append(".");

                String latLong1 = parts[index++];
                latitude.append(latLong1.substring(0, 1));
                longitude.append(latLong1.substring(1, 2));

                String latLong2 = parts[index++];
                latitude.append(latLong2.substring(0, 1));
                longitude.append(latLong2.substring(1, 2));

                String latLong3 = parts[index++];
                latitude.append(latLong3.substring(0, 1));
                longitude.append(latLong3.substring(1, 2));

                GeoHash geoHash = new GeoHash();
                geoHash.latitude = Double.parseDouble(latitude.toString());
                geoHash.longitude = Double.parseDouble(longitude.toString());
                maybeGeoHash = Optional.of(geoHash);
            }
            else {
                log.debug("Could not parse latitude & longitude from {}", firstLatLong);
            }
        }

        return maybeGeoHash;
    }


    static String createUniqueVehicleId(int ownerOperatorId, int vehicleNumber) {
        return ownerOperatorId + "/" + vehicleNumber;
    }

    static String joinFirstNParts(String[] parts, int upToIndexExcludingThis, String delimiter) {
        StringBuffer buffer = new StringBuffer();
        int index = 0;

        buffer.append(delimiter);
        while (index < upToIndexExcludingThis - 1) {
            index++;
            buffer.append(parts[index]);
            buffer.append(delimiter);
        }
        return buffer.toString();
    }

    public static int findVersionIndex(String[] parts) {
        for (int n = 0; n < parts.length; n++) {
            String p = parts[n];
            if (topicVersionRegex.matcher(p).matches()) {
                return n;
            }
        }
        return -1;
    }

    static String validateString(String str) {
        if (str == null || str.isEmpty())
            return null;
        else
            return str;
    }

    static Integer safeParseInt(String n) {
        if (n == null || n.isEmpty())
            return null;
        else {
            try {
                return Integer.parseInt(n);
            }
            catch (NumberFormatException e) {
                log.error("Failed to convert {} to integer", n);
                return null;
            }
        }
    }

    static Boolean safeParseBoolean(Integer n) {
        if (n == null)
            return null;
        else
            return n != 0;
    }

    static Time safeParseTime(String time) {
        if (time == null)
            return null;
        else {
            try {
                return Time.valueOf(time + ":00"); // parser requires seconds also.
            }
            catch (Exception e) {
                log.error("Failed to convert {} to java.sql.Time", time);
                return null;
            }
        }
    }

    static LocalTime safeParseLocalTime(String time) {
        if (time == null)
            return null;
        else {
            try {
                return LocalTime.parse(time);
            }
            catch (Exception e) {
                log.error("Failed to convert {} to LocalTime", time);
                return null;
            }
        }
    }

    static Timestamp safeParseTimestamp(String dt) {
        if (dt == null)
            return null;
        else {
            try {
                OffsetDateTime offsetDt = OffsetDateTime.parse(dt);
                return new Timestamp(offsetDt.toEpochSecond() * 1000L);
            }
            catch (Exception e) {
                log.error("Failed to convert {} to java.sql.Timestamp", dt);
                return null;
            }
        }
    }
}
