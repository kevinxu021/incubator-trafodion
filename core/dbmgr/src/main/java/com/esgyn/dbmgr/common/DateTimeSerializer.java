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

public class DateTimeSerializer extends JsonSerializer<DateTime> {
  public static final DateTimeFormatter formatter =
      DateTimeFormat.forPattern(DateSerializer.ISO_DATE_SIMPLE_FORMAT);

  @Override
  public void serialize(DateTime value, JsonGenerator generator, SerializerProvider provider)
      throws IOException, JsonProcessingException {

    generator.writeString(formatter.print(value));
  }

}
