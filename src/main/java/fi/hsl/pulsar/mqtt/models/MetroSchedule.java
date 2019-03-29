package fi.hsl.pulsar.mqtt.models;

import java.util.List;

public class MetroSchedule {
    public int schemaVersion;
    public String routeName;
    public MetroTrainType trainType;
    public MetroProgress journeySectionprogress;
    public String beginTime;
    public String endTime;
    public List<MetroScheduleRouteRow> routeRows;
}
