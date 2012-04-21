package com.plant42.log4j.layouts;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;


/**
 Copyright (c) 2011 Stuart Clark, http://www.plant42.com/

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 A Log4j layout that formats a LoggingEvent to a JSONified string.
 */

public class JSONLayout extends Layout {


    /**
     * format a given LoggingEvent to a string, in this case JSONified string
     * @param loggingEvent
     * @return String representation of LoggingEvent
     */
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


    /**
     * Converts LoggingEvent Throwable to JSON object
     * @param json
     * @param event
     * @throws JSONException
     */
    private void writeThrowable(JSONObject json, LoggingEvent event) throws JSONException {
        ThrowableInformation ti = event.getThrowableInformation();
        if (ti != null) {
            Throwable t = ti.getThrowable();
            JSONObject throwable = new JSONObject();

            throwable.put("message", t.getMessage());
            throwable.put("className", t.getClass().getCanonicalName());
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
            json.put("throwable", throwable);
        }
    }


    /**
     * Converts basic LogginEvent properties to JSON object
     * @param json
     * @param event
     * @throws JSONException
     */
    private void writeBasic(JSONObject json, LoggingEvent event) throws JSONException {
        json.put("threadName", event.getThreadName());
        json.put("level", event.getLevel().toString());
        json.put("timestamp", event.getTimeStamp());
        json.put("message", event.getMessage());
        json.put("logger", event.getLoggerName());
    }

    /**
     * Declares that this layout does not ignore throwable if available
     * @return
     */
    @Override
    public boolean ignoresThrowable() {
        return false;
    }

    /**
     * Just fulfilling the interface/abstract class requirements
     */
    @Override
    public void activateOptions() {
    }

}
