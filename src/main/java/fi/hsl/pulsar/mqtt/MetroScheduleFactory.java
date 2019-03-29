package fi.hsl.pulsar.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.MetroMessages;
import fi.hsl.pulsar.mqtt.models.MetroProgress;
import fi.hsl.pulsar.mqtt.models.MetroSchedule;
import fi.hsl.pulsar.mqtt.models.MetroScheduleRouteRow;
import fi.hsl.pulsar.mqtt.models.MetroTrainType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetroScheduleFactory implements IMapperFactory {

    private static final Logger log = LoggerFactory.getLogger(MetroScheduleFactory.class);

    private final static Map<String, String> properties;
    private final static ObjectMapper mapper = new ObjectMapper();
    private static final Pattern pattern = Pattern.compile("^metro-mipro-ats\\/v1\\/schedule\\/(.+)\\/(.+)$");

    static {
        HashMap<String, String> props = new HashMap<>();
        props.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.MetroSchedule.toString());
        props.put(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(MetroMessages.Schedule.newBuilder().getSchemaVersion()));
        properties = Collections.unmodifiableMap(props);
    }

    @Override
    public BiFunction<String, byte[], byte[]> createMapper() {
        return (topic, payload) -> {
            MetroSchedule schedule = null;
            try {
                schedule = mapper.readValue(payload, MetroSchedule.class);
            } catch (Exception e) {
                log.error("Failed to parse payload {} from topic {}, {}", new String(payload), topic, e);
            }
            MetroMessages.Schedule raw = createSchedule(schedule, topic);
            return raw.toByteArray();
        };
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    private static String[] parseTopic(final String topic) throws ParseException {
        Matcher matcher = pattern.matcher(topic);
        if (!matcher.matches()) {
            throw new ParseException(String.format("Failed to parse topic %s.", topic), 0);
        }
        return new String[]{matcher.group(1), matcher.group(2)};
    }

    private static MetroMessages.Schedule createSchedule(final MetroSchedule schedule, final String topic) {
        String[] topicParams = {null, null};
        try {
            topicParams = parseTopic(topic);
        } catch (Exception e) {
            log.error("Failed to parse topic {}, {}", topic, e);
        }
        MetroMessages.Schedule.Builder builder = MetroMessages.Schedule.newBuilder();
        MetroMessages.Schedule raw = builder
                .setSchemaVersion(builder.getSchemaVersion())
                .setRouteName(schedule.routeName)
                .setTrainType(createTrainType(schedule.trainType))
                .setJourneySectionprogress(createProgress(schedule.journeySectionprogress))
                .setBeginTime(schedule.beginTime)
                .setEndTime(schedule.endTime)
                .addAllRouteRows(schedule.routeRows
                        .stream()
                        .map(MetroScheduleFactory::createScheduleRouteRow)
                        .collect(Collectors.toList())
                )
                .setShiftNumber(topicParams[0])
                .setJsId(topicParams[1])
                .build();
        return raw;
    }

    private static MetroMessages.RouteRow createScheduleRouteRow(final MetroScheduleRouteRow routeRow) {
        MetroMessages.RouteRow.Builder builder = MetroMessages.RouteRow.newBuilder();
        MetroMessages.RouteRow raw = builder
                .setRouterowId(routeRow.routerowId)
                .setStation(routeRow.station)
                .setPlatform(routeRow.platform)
                .setArrivalTimePlanned(routeRow.arrivalTimePlanned)
                .setArrivalTimeForecast(routeRow.arrivalTimeForecast)
                .setArrivalTimeMeasured(routeRow.arrivalTimeMeasured)
                .setDepartureTimePlanned(routeRow.departureTimePlanned)
                .setDepartureTimeForecast(routeRow.departureTimeForecast)
                .setDepartureTimeMeasured(routeRow.departureTimeMeasured)
                .setSource(routeRow.source)
                .setRowProgress(createProgress(routeRow.rowProgress))
            .build();
        return raw;
    }

    private static MetroMessages.Progress createProgress(final MetroProgress progress) {
        switch (progress) {
            case SCHEDULED: return MetroMessages.Progress.SCHEDULED;
            case INPROGRESS: return MetroMessages.Progress.INPROGRESS;
            case COMPLETED: return MetroMessages.Progress.COMPLETED;
            case CANCELLED: return MetroMessages.Progress.CANCELLED;
            default: throw new IllegalArgumentException(String.format("Failed to parse MetroProgress %s.", progress));
        }
    }

    private static MetroMessages.TrainType createTrainType(final MetroTrainType trainType) {
        switch (trainType) {
            case M: return MetroMessages.TrainType.M;
            case T: return MetroMessages.TrainType.T;
            default: throw new IllegalArgumentException(String.format("Failed to parse MetroTrainType %s.", trainType));
        }
    }
}
