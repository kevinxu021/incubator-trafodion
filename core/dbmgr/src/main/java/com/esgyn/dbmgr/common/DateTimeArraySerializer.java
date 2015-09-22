// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.common;

import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DateTimeArraySerializer extends JsonSerializer<DateTime[]> {

  public static final String ISO_DATE_SIMPLE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final DateTimeFormatter formatter =
      DateTimeFormat.forPattern(ISO_DATE_SIMPLE_FORMAT);

  @Override
  public void serialize(DateTime[] dateTimes, JsonGenerator generator, SerializerProvider provider)
      throws IOException, JsonProcessingException {
    String[] stringArray = new String[dateTimes.length];
    for (int i = 0; i < dateTimes.length; i++) {
      if (dateTimes[i] != null) {
        stringArray[i] = formatter.print(dateTimes[i]);
      }
    }
    generator.writeObject(stringArray);
  }

}
