package it.uniroma2.utils.time;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class MonthTimeInterval implements TimeInterval{
    private static final String timeType = "month";

    @Override
    public String getTimeType() {
        return timeType;
    }

    @Override
    public TumblingEventTimeWindows getTimeIntervalClass() {
        return new MonthWindowAssigner();
    }

    public static class MonthWindowAssigner extends TumblingEventTimeWindows{

        public MonthWindowAssigner() {
            super(1, 0, WindowStagger.ALIGNED);
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
            Calendar calendar = Calendar.getInstance();

            calendar.setTime(new Date(timestamp));
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            long startDate = calendar.getTimeInMillis();
            calendar.add(Calendar.MONTH,1);
            return Collections.singletonList(new TimeWindow(startDate, calendar.getTimeInMillis()));
        }
    }
}
