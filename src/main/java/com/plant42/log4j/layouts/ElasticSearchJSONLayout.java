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

public class ElasticSearchJSONLayout extends JSONLayout {


    private String index = "json-index";
    private String type = "json";


    /**
     * format a given LoggingEvent to a string, in this case JSONified string
     * @param loggingEvent
     * @return String representation of LoggingEvent
     */
    @Override
    public String format(LoggingEvent loggingEvent) {
        
        StringBuilder sb = new StringBuilder();

        JSONObject action = new JSONObject();
        JSONObject source = new JSONObject();

        try {
            JSONObject actionContent = new JSONObject();
            actionContent.put("_index", this.index);
            actionContent.put("_type", this.type);
            action.put("index", actionContent);




            JSONObject sourceContent = new JSONObject();

            //== write basic fields
            writeBasic(sourceContent, loggingEvent);

            //== write throwable fields
            writeThrowable(sourceContent, loggingEvent);

            source.put(this.type, sourceContent);

        } catch (JSONException e) {
            e.printStackTrace();
        }

        sb.append(action.toString());
        sb.append("\n");
        sb.append(source.toString());
        sb.append("\n");

        return sb.toString();

    }


    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
