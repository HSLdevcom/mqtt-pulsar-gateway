package fi.hsl.pulsar.mqtt.hfp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Optional;

public class HfpMetadata {
    private static final Logger log = LoggerFactory.getLogger(HfpMetadata.class);

    public enum JourneyType {
        journey, deadrun
    }

    public enum TransportMode {
        bus, train, tram, metro, ferry;

        public static Optional<TransportMode> fromString(String str) {
            // There is a Fara VPC bug where the mode is sometimes missing.
            if (str == null || str.isEmpty()) {
                log.warn("Could not parse TransportMode because it's empty");
                return Optional.empty();
            }
            try {
                return Optional.of(TransportMode.valueOf(str));
            }
            catch (Exception e) {
                log.warn("Could not parse TransportMode: " + str);
                return Optional.empty();
            }
        }
    }

    public OffsetDateTime received_at;
    public String topic_prefix;
    public String topic_version;
    public JourneyType journey_type;
    public boolean is_ongoing;
    public Optional<TransportMode> mode = Optional.empty();
    public int owner_operator_id;
    public int vehicle_number;
    public String unique_vehicle_id;
    public Optional<String> route_id  = Optional.empty();
    public Optional<Integer> direction_id = Optional.empty();
    public Optional<String> headsign = Optional.empty();
    public Optional<LocalTime> journey_start_time = Optional.empty();
    public Optional<String> next_stop_id = Optional.empty();
    public Optional<Integer> geohash_level = Optional.empty();
    public Optional<Double> topic_latitude = Optional.empty();
    public Optional<Double> topic_longitude = Optional.empty();

}
