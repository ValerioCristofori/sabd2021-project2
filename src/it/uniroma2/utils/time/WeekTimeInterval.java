package it.uniroma2.utils.time;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class WeekTimeInterval implements TimeInterval{

    private static final String timeType = "week";

    @Override
    public String getTimeType() {
        return timeType;
    }

    @Override
    public TumblingEventTimeWindows getTimeIntervalClass() {
        return new WeekWindowAssigner();
    }

    public static class WeekWindowAssigner extends TumblingEventTimeWindows{

        public WeekWindowAssigner() {
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
            calendar.set(Calendar.DAY_OF_WEEK, 1);
            long startDate = calendar.getTimeInMillis();
            calendar.add(Calendar.WEEK_OF_YEAR,1);
            return Collections.singletonList(new TimeWindow(startDate, calendar.getTimeInMillis()));
        }
    }
}
