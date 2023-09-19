package com.cap.service.batch.app.interceptor;

import cn.hutool.json.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1获取body里的数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        //2判断是不是合法数据
        if (JSONUtil.isJson(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while(iterator.hasNext()){
           Event event =  iterator.next();
           if (intercept(event) == null){
               iterator.remove();
           }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return null;
        }

        @Override
        public void configure(Context context) {

        }
    }
}
