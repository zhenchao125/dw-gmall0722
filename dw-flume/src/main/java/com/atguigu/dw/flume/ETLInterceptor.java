package com.atguigu.dw.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2019/11/12 14:26
 */
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 只做简单etl
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }
        byte[] body = event.getBody();
        String msg = new String(body, Charset.forName("utf-8"));
        if (msg.contains("\"en\":\"start\"")) {  // 启动日志
            if (LogUitl.validateStart(msg)) {
                return event;
            }
        } else {  // 时间日志
            if (LogUitl.validateEvent(msg)) {
                return event;
            }
        }
        return null;
    }

    /**
     * 返回值必须是原来的list
     * 可以把要拦截的event从list中移除
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> it = events.iterator();
        if (it.hasNext()) {
            Event event = it.next();
            if (intercept(event) == null) {
                it.remove();
            }
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
