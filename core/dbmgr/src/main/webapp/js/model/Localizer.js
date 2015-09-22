// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([ 'jquery',
         'jquerylocalize'
         ],function($) {
	"use strict";

	var Localizer = (function() {
		var localizedData = {};
		
		function Localizer() {

			var lng = window.navigator.userLanguage || window.navigator.language;
			$("[data-localize]").localize("dbmgr", { language: lng, pathPrefix: "lang", fallback: "en", callback: function(data, defaultCallback){
				localizedData = data;
		        defaultCallback(data)
		    }});
			
			this.get = function(s) {
				if(localizedData.hasOwnProperty(s))
					return localizedData[s];
			}
		}
		return new Localizer();
	}());
	return Localizer;
});
