package fi.hsl.pulsar.mqtt.hfp;

import org.junit.Test;

import java.net.URL;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.Scanner;

import static org.junit.Assert.*;

public class MessageParserTest {

    @Test
    public void parseTimestampSafely() {
        Timestamp ts = MessageParser.safeParseTimestamp("2018-04-05T17:38:36Z");
        assertEquals(1522949916000L, ts.getTime());

        Timestamp missingTimezone = MessageParser.safeParseTimestamp("2018-04-05T17:38:36");
        assertNull(missingTimezone);

        assertNull(MessageParser.safeParseTimestamp("datetime"));
        assertNull(MessageParser.safeParseTimestamp(null));
    }

    @Test
    public void parseTimeSafely() {
        Time time = MessageParser.safeParseTime("18:00");
        assertTrue(time.toLocalTime().equals(LocalTime.of(18, 0)));

        Time earlyTime = MessageParser.safeParseTime("8:00");
        assertTrue(earlyTime.toLocalTime().equals(LocalTime.of(8, 0)));

        Time earlyTime2 = MessageParser.safeParseTime("08:00");
        assertTrue(earlyTime2.toLocalTime().equals(LocalTime.of(8, 0)));

        assertNull(MessageParser.safeParseTime("random-time"));
        assertNull(MessageParser.safeParseTime(null));
    }

    @Test
    public void parseSampleFile() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource("hfp-sample.json");

        String content = new Scanner(url.openStream(), "UTF-8").useDelimiter("\\A").next();

        HfpMessage hfp = MessageParser.newInstance().parse(content.getBytes("UTF-8"));
        assertNotNull(hfp);
        assertEquals("81", hfp.VP.desi);
        assertEquals("2", hfp.VP.dir);
        assertTrue(22 == hfp.VP.oper);
        assertTrue(792 == hfp.VP.veh);
        assertEquals("2018-04-05T17:38:36Z", hfp.VP.tst);
        assertTrue(1522949916 == hfp.VP.tsi);
        assertTrue(0.16 - hfp.VP.spd < 0.00001f);
        assertTrue(225 == hfp.VP.hdg);
        assertTrue(60.194481 - hfp.VP.lat < 0.00001f);
        assertTrue(25.03095 - hfp.VP.longitude < 0.00001f);
        assertTrue(0 == hfp.VP.acc);
        assertTrue(-25 == hfp.VP.dl);
        assertTrue(2819 - hfp.VP.odo < 0.00001f);
        assertTrue(0 == hfp.VP.drst);
        assertEquals(java.sql.Date.valueOf("2018-04-05"), hfp.VP.oday);
        assertTrue(636 == hfp.VP.jrn);
        assertTrue(112 == hfp.VP.line);
        assertEquals("20:25", hfp.VP.start);
    }

    @Test
    public void parseTopic() throws Exception {
        HfpMetadata meta = parseAndValidateTopic("/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals(HfpMetadata.JourneyType.journey, meta.journey_type);
        assertEquals(true, meta.is_ongoing);
        assertEquals(HfpMetadata.TransportMode.bus, meta.mode.get());
        assertEquals(22, meta.owner_operator_id);
        assertEquals(854, meta.vehicle_number);
        assertEquals(MessageParser.createUniqueVehicleId(22, 854), meta.unique_vehicle_id);

        assertEquals("4555B", meta.route_id.get());
        assertEquals(2, (int)meta.direction_id.get());
        assertEquals("Leppävaara", meta.headsign.get());
        assertEquals(LocalTime.of(19, 56), meta.journey_start_time.get());
        assertEquals("4150264", meta.next_stop_id.get());
        assertEquals(5, (int)meta.geohash_level.get());

        assertTrue(60.260 - meta.topic_latitude.get() < 0.00001);
        assertTrue(24.856 - meta.topic_longitude.get() < 0.00001);
    }

    @Test
    public void parseMissingGeohash() throws Exception {
        ///hfp/v1/journey/ongoing/bus/0012/01328/4560/1/Myyrmäki/04:57/4160299/0////
        HfpMetadata meta = parseAndValidateTopic("/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/0////");
        assertEquals(0, (int)meta.geohash_level.get());
        assertFalse(meta.topic_latitude.isPresent());
        assertFalse(meta.topic_longitude.isPresent());
    }

    @Test
    public void parseGeohashWithOverloadedZeroLevel() throws Exception {
        HfpMetadata meta = parseAndValidateTopic("/hfp/v1/journey/ongoing/bus/0012/01825/1039/2/Kamppi/05:36/1320105/0/60;24/28/44/12");
        assertEquals(0, (int)meta.geohash_level.get());
        assertTrue(60.241 - meta.topic_latitude.get() < 0.00001);
        assertTrue(24.842 - meta.topic_longitude.get() < 0.00001);
    }

    @Test
    public void parseTopicWhenItemsMissing() throws Exception {
        HfpMetadata meta = parseAndValidateTopic("/hfp/v1/journey/ongoing//0022/00854//////////");
        assertEquals(HfpMetadata.JourneyType.journey, meta.journey_type);
        assertEquals(true, meta.is_ongoing);
        assertFalse(meta.mode.isPresent());

        assertEquals(22, meta.owner_operator_id);
        assertEquals(854, meta.vehicle_number);
        assertEquals(MessageParser.createUniqueVehicleId(22, 854), meta.unique_vehicle_id);

        assertFalse(meta.route_id.isPresent());
        assertFalse(meta.direction_id.isPresent());
        assertFalse(meta.headsign.isPresent());
        assertFalse(meta.journey_start_time.isPresent());
        assertFalse(meta.next_stop_id.isPresent());
        assertFalse(meta.geohash_level.isPresent());

        assertFalse(meta.topic_latitude.isPresent());
        assertFalse(meta.topic_longitude.isPresent());
    }

    @Test
    public void parseTopicWhenPrefixLonger() throws Exception {
        HfpMetadata meta = parseAndValidateTopic("/hsldevcom/public/hfp/v1/deadrun/ongoing/tram/0022/00854////08:08///60;24/28/65/06");
        assertEquals(HfpMetadata.JourneyType.deadrun, meta.journey_type);
        assertEquals(true, meta.is_ongoing);
        assertEquals(HfpMetadata.TransportMode.tram, meta.mode.get());

        assertEquals(22, meta.owner_operator_id);
        assertEquals(854, meta.vehicle_number);
        assertEquals(MessageParser.createUniqueVehicleId(22, 854), meta.unique_vehicle_id);

        assertFalse(meta.route_id.isPresent());
        assertFalse(meta.direction_id.isPresent());
        assertFalse(meta.headsign.isPresent());
        assertEquals(LocalTime.of(8, 8), meta.journey_start_time.get());
        assertFalse(meta.next_stop_id.isPresent());
        assertFalse(meta.geohash_level.isPresent());

        assertTrue(60.260 - meta.topic_latitude.get() < 0.00001);
        assertTrue(24.856 - meta.topic_longitude.get() < 0.00001);

    }

    private HfpMetadata parseAndValidateTopic(String topic) throws Exception {
        OffsetDateTime now = OffsetDateTime.now();
        Optional<HfpMetadata> maybeMeta = MessageParser.parseMetadata(topic, now);
        assertTrue(maybeMeta.isPresent());
        HfpMetadata meta = maybeMeta.get();
        assertEquals(now, meta.received_at);
        assertEquals("v1", meta.topic_version);
        return meta;
    }

    @Test
    public void testTopicPrefixParsing() throws Exception {
        String prefix = parseTopicPrefix("/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals("/hfp/", prefix);
        String emptyPrefix = parseTopicPrefix("/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals("/", emptyPrefix);
        String longerPrefix = parseTopicPrefix("/hsldevcomm/public/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals("/hsldevcomm/public/hfp/", longerPrefix);

    }

    private String parseTopicPrefix(String topic) throws Exception {
        final String[] allParts = topic.split("/");
        int versionIndex = MessageParser.findVersionIndex(allParts);
        return MessageParser.joinFirstNParts(allParts, versionIndex, "/");
    }
}
