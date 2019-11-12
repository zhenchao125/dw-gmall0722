package com.atguigu.dw.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2019/11/12 14:26
 */
public class TypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 不同的事件, 在header添加一个field
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("utf-8"));
        Map<String, String> headers = event.getHeaders();
        if (body.contains("start")) {
            headers.put("topic", "topic_start");
        } else {
            headers.put("topic", "topic_event");

        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
