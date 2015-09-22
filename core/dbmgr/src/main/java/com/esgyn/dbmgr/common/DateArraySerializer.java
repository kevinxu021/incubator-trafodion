// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.dbmgr.common;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DateArraySerializer extends JsonSerializer<Date[]> {

  @Override
  public void serialize(Date[] dates, JsonGenerator generator, SerializerProvider provider)
      throws IOException, JsonProcessingException {

    String[] stringArray = new String[dates.length];
    for (int i = 0; i < dates.length; i++) {
      if (dates[i] != null) {
        SimpleDateFormat formatter = new SimpleDateFormat(DateSerializer.ISO_DATE_SIMPLE_FORMAT);
        stringArray[i] = formatter.format(dates[i]);
      }
    }
    generator.writeObject(stringArray);
  }

}
