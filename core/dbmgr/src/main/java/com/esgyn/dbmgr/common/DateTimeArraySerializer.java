// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Hewlett-Packard Development Company, L.P.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
