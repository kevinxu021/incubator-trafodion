package com.esgyn.dbmgr.common;
/*
 * Copyright (c) 1995, 2008, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */ 


import java.util.*;

public class EsgynLocalizeMapping {
	
  
	public static String getLocalizedValue(String language,String key) {
		Locale locale=getLocale(language);
      ResourceBundle resource = 
    	         ResourceBundle.getBundle("dbmgrBundle",locale);
      String value  = resource.getString(key);
      return value;
   }
   static public void main(String[] args) {
      
      System.out.println(getLocalizedValue("zh-CN","s2"));

   } 
   private static Locale getLocale(String language) {
	   Locale tmpLocale=null;
	   switch (language) {
	case "fr":
		tmpLocale=Locale.FRANCE;
		break;
	case "en":
		tmpLocale=Locale.ENGLISH;
		break;
	case "de":
		tmpLocale=Locale.GERMANY;
		break;
	case "zh-CN":
		tmpLocale=Locale.SIMPLIFIED_CHINESE;
		break;
	default:
		tmpLocale=Locale.ENGLISH;
		break;
	}
	   return tmpLocale;
	}

} 
