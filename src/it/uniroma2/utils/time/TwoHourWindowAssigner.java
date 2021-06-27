package it.uniroma2.utils.time;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class TwoHourWindowAssigner extends TumblingEventTimeWindows{

        public TwoHourWindowAssigner() {
            super(1, 0, WindowStagger.ALIGNED);
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
            Calendar calendar = Calendar.getInstance();

            calendar.setTime(new Date(timestamp));
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.set(Calendar.HOUR_OF_DAY, 1);
            long startDate = calendar.getTimeInMillis();
            calendar.add(Calendar.HOUR,2);
            return Collections.singletonList(new TimeWindow(startDate, calendar.getTimeInMillis()));
        }
    }
