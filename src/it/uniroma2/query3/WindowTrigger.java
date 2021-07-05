package it.uniroma2.query3;

import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowTrigger extends Trigger<Object, TimeWindow> {

    private final EventTimeTrigger eventTimeTrigger;

    private WindowTrigger(){
        this.eventTimeTrigger = EventTimeTrigger.create();
    }

    @Override
    public boolean canMerge() {
        return this.eventTimeTrigger.canMerge();
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        this.eventTimeTrigger.onMerge(window, ctx);
    }

    @Override
    public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        TriggerResult result = this.eventTimeTrigger.onElement(o, l, timeWindow, triggerContext);
        if( result == TriggerResult.FIRE) return TriggerResult.FIRE_AND_PURGE;
        else return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return eventTimeTrigger.onProcessingTime(l, timeWindow, triggerContext);    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        TriggerResult result = this.eventTimeTrigger.onEventTime(l, timeWindow, triggerContext);
        if( result == TriggerResult.FIRE) return TriggerResult.FIRE_AND_PURGE;
        else return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        this.eventTimeTrigger.clear(timeWindow, triggerContext);
    }

    public static WindowTrigger createInstance(){
        return new WindowTrigger();
    }
}
