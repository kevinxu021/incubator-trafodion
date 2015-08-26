//@@@ START COPYRIGHT @@@

//(C) Copyright 2013-2015 Hewlett-Packard Development Company, L.P.

//@@@ END COPYRIGHT @@@
//JS used to store some common JS functions and constant

define(['moment'],
function(moment) {
	"use strict";

	return (function() {

		function Common() {

			// var _isoDateFormat='yyyy-MM-dd HH:mm:ss'
			this.ISODateFormat = 'YYYY-MM-DD HH:mm:ss';
			var _this = this;

			this.formatNumberWithCommas = function(number) {
				if(number == null)
					return "";
				return number.toString().replace(/,/g, "").replace(/\B(?=(\d{3})+(?!\d))/g, ",");
			};

			this.toUTCFromMilliSeconds = function(milliSeconds) {
				if (milliSeconds != null) {
					return moment(milliSeconds).utc().format(_this.ISODateFormat);
				}
				return "";
			},
			this.toDateFromMilliSeconds = function(milliSeconds) {
				if (milliSeconds != null) {
					return moment(milliSeconds).format(_this.ISODateFormat);
				}
				return "";
			},

			this.getTimeZoneOffset = function(localeTimeZone) {
				return moment().tz(localeTimeZone).zone() * 60 * 1000;
			},

			this.getBrowserTimeZoneOffset = function() {
				return moment().zone() * 60 * 1000;
			},

			this.bytesToSize = function(bytes) {
				var units = [ 'bytes', 'KB', 'MB', 'GB', 'TB' ];
				if (bytes <= 0)
					return bytes;

				if (bytes < 1)
					return bytes + ' bytes';

				if (bytes == 1)
					return bytes + ' byte';

				if (bytes <= 1)
					return bytes + ' bytes';
				var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
				if (i <= 0)
					return bytes.toFixed(1) + ' ' + units[0];
				return (bytes / Math.pow(1024, i)).toFixed(1) + units[i];
			};

			this.convertToMB = function(bytes){
				if(bytes <=0)
					return 0;
				return (bytes/1024/1024).toFixed(2);
			};
			
			this.convertUnitFromByte = function(bytes) {
				var memory = {};
				var units = [ 'bytes', 'KB', 'MB', 'GB', 'TB' ];

				if (bytes < 1)
				{
					memory.size =  bytes;
					memory.unit = units[0];
					return memory;
				}

				if (bytes == 1)
				{
					memory.size = bytes;
					memory.unit = units[0];
					return memory;
				}

				var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
				if (i == 0)
				{
					memory.size = bytes
					memory.unit = units[0];
					return memory;
				}
				memory.size = parseFloat((bytes / Math.pow(1024, i)).toFixed(0));
				memory.unit = units[i];
				return memory;
			};

			this.bytesToMB = function(bytes) {
				var sizes = [ 'bytes', 'KB', 'MB' ];
				if (bytes <= 0)
					return bytes;
				if (bytes < 1)
					return bytes + ' bytes';
				if (bytes == 1)
					return bytes + ' byte';
				var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
				if (i == 0)
					return _this.formatNumberWithCommas(bytes) + ' ' + sizes[i];
				if(i <= 2)
					return _this.formatNumberWithCommas((bytes / Math.pow(1024, i)).toFixed(1)) +  sizes[i];
				else 
					return _this.formatNumberWithCommas((bytes / Math.pow(1024, 2)).toFixed(1)) +  sizes[2];
			};
			
			this.toProperCase = function(s) {
				return s.toLowerCase().replace(/^(.)|\s(.)/g, function($1) {
					return $1.toUpperCase();
				});
			};
		}
		return new Common();
	})();

});
