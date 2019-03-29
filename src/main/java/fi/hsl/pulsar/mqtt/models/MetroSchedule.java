package fi.hsl.pulsar.mqtt.models;

import java.util.List;

public class MetroSchedule {
    public int schemaVersion;
    public String routeName;
    public TrainType trainType;
    public Progress journeySectionprogress;
    public String beginTime;
    public String endTime;
    public List<RouteRow> routeRows;
}

enum TrainType {
    M, T
}

enum Progress {
    SCHEDULED, INPROGRESS, COMPLETED, CANCELLED
}

class RouteRow {
    public long routerowId;
    public String station;
    public String platform;
    public String arrivalTimePlanned;
    public String arrivalTimeForecast;
    public String arrivalTimeMeasured;
    public String departureTimePlanned;
    public String departureTimeForecast;
    public String departureTimeMeasured;
    public String source;
    public Progress rowProgress;
}
