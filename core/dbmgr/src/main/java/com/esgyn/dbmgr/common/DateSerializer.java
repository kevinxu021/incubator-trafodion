package com.esgyn.dbmgr.common;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DateSerializer extends JsonSerializer<Date> {

  public static final String ISO_DATE_SIMPLE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  /**
   * Serialize java.util.Date into String
   */
  @Override
  public void serialize(Date date, JsonGenerator generator, SerializerProvider provider)
      throws IOException {
    SimpleDateFormat formatter = new SimpleDateFormat(ISO_DATE_SIMPLE_FORMAT);
    generator.writeString(formatter.format(date));
  }

}
