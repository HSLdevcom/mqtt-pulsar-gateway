package fi.hsl.pulsar.mqtt.hfp;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalTime;
import java.time.OffsetDateTime;

// ignore unknown properties (default for objects).
// to disallow unknown properties in JSON set it to FAIL which will result in exception instead
@CompiledJson(onUnknown = CompiledJson.Behavior.IGNORE)
public class HfpMessage {
    //Specification: https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/
    //Example payload:
    // {"VP":{"desi":"81","dir":"2","oper":22,"veh":792,"tst":"2018-04-05T17:38:36Z","tsi":1522949916,"spd":0.16,"hdg":225,"lat":60.194481,"long":25.03095,"acc":0,"dl":-25,"odo":2819,"drst":0,"oday":"2018-04-05","jrn":636,"line":112,"start":"20:25"}}

    @JsonAttribute(nullable = false)
    public Payload VP;

    @CompiledJson(onUnknown = CompiledJson.Behavior.IGNORE)
    static class Payload {

        public String desi;

        public String dir;

        public Integer oper;

        public Integer veh;

        //See possible use of converter directly to OffsetDateTime
        //@JsonAttribute(converter = converter.class)
        @JsonAttribute(nullable = false)
        public String tst;

        @JsonAttribute(nullable = false)
        public long tsi;

        public Double spd;

        public Double hdg;

        public Double lat;

        @JsonAttribute(name = "long") //use alternative name in JSON
        public Double longitude;

        public Double acc;

        public Integer dl;

        public Double odo;

        //Boolean:
        public Integer drst;

        public Date oday;

        public Integer jrn;

        public Integer line;

        //TODO parse to LocalTime using format %H:%M in 24 hour clock
        public String start;
    }

}
