package com.plant42.log4j.layouts;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;


public class JSONLayout extends Layout {

    @Override
    public String format(LoggingEvent loggingEvent) {

        JSONObject root = new JSONObject();

        try {
            //== write basic fields
            writeBasic(root, loggingEvent);

            //== write throwable fields
            writeThrowable(root, loggingEvent);

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return root.toString();
    }


    private void writeThrowable(JSONObject j, LoggingEvent event) throws JSONException {
        ThrowableInformation ti = event.getThrowableInformation();
        if (ti != null) {
            Throwable t = ti.getThrowable();
            JSONObject json = new JSONObject();
            
            json.put("message", t.getMessage());
            json.put("className", t.getClass().getCanonicalName());
            List<JSONObject> traceObjects = new ArrayList<JSONObject>();
            for(StackTraceElement ste : t.getStackTrace()) {
                JSONObject element = new JSONObject();
                element.put("class", ste.getClassName());
                element.put("method", ste.getMethodName());
                element.put("line", ste.getLineNumber());
                element.put("file", ste.getFileName());
                traceObjects.add(element);
            }
            
            json.put("stackTrace", traceObjects);
            j.put("throwable", json);
        }
    }


    private void writeBasic(JSONObject j, LoggingEvent event) throws JSONException {
        j.put("threadName", event.getThreadName());
        j.put("level", event.getLevel().toString());
        j.put("timestamp", event.getTimeStamp());
        j.put("message", event.getMessage());
        j.put("logger", event.getLoggerName());
    }
    
    @Override
    public boolean ignoresThrowable() {
        return false;
    }

    @Override
    public void activateOptions() {
    }

}
