// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var ToolsHandler = ( function() {
			var xhrs = [];

			function ToolsHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;
				this.CREATE_LIBRARY_SUCCESS = 'createLibrarySuccess';
				this.CREATE_LIBRARY_ERROR = 'createLibraryError';
				this.ALTER_LIBRARY_SUCCESS = 'alterLibrarySuccess';
				this.ALTER_LIBRARY_ERROR = 'alterLibraryError';
				this.GET_LIBRARY_SUCCESS = 'getLibrarySuccess';
				this.GET_LIBRARY_ERROR = 'getLibraryError';

				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.createLibrary = function(file, fileName, filePart, fileSize, schemaName, libraryName,oflag, sflag, eflag, uflag){
					_this.fileSize=fileSize;
					var fd = new FormData();
					fd.append("file", file);
					fd.append("fileName", fileName);
					fd.append("filePart", filePart);
					fd.append("schemaName", schemaName);
					fd.append("libraryName", libraryName);
					fd.append("overwriteFlag", oflag);
					fd.append("startFlag", sflag);
					fd.append("endFlag", eflag);
					fd.append("updateFlag", uflag);
					
					$.ajax({
						url: 'resources/tools/createlibrary',
						//url: 'resources/tools/upload',
						type:'POST',
						data: fd,
						processData : false,
						contentType : false, 
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.data = data;
							result.schemaName = schemaName;
							result.libraryName = libraryName;
							dispatcher.fire(_this.CREATE_LIBRARY_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.schemaName = schemaName;
							jqXHR.libraryName = libraryName;
							dispatcher.fire(_this.CREATE_LIBRARY_ERROR, jqXHR, res, error);
						}
					});
				
				};
				
				this.alterLibrary = function(file, fileName, filePart, fileSize, schemaName, libraryName,oflag, sflag, eflag, uflag){
					_this.fileSize=fileSize;
					var fd = new FormData();
					fd.append("file", file);
					fd.append("fileName", fileName);
					fd.append("filePart", filePart);
					fd.append("schemaName", schemaName);
					fd.append("libraryName", libraryName);
					fd.append("overwriteFlag", oflag);
					fd.append("startFlag", sflag);
					fd.append("endFlag", eflag);
					fd.append("updateFlag", uflag);
					
					$.ajax({
						url: 'resources/tools/createlibrary',
						//url: 'resources/tools/upload',
						type:'POST',
						data: fd,
						processData : false,
						contentType : false, 
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.data = data;
							result.schemaName = schemaName;
							result.libraryName = libraryName;
							dispatcher.fire(_this.ALTER_LIBRARY_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.schemaName = schemaName;
							jqXHR.libraryName = libraryName;
							dispatcher.fire(_this.ALTER_LIBRARY_ERROR, jqXHR, res, error);
						}
					});
				
				};
				
				this.getLibrary = function(fileName){
					var fd = new FormData();
					fd.append("fileName", fileName);
					
					$.ajax({
						url: 'resources/tools/getlibrary',
						type:'POST',
						data: fd,
						processData : false,
						contentType : false, 
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						fileName: fileName,
						success: function(data){
							var url = window.location.protocol + "//" + window.location.host+"/"+this.fileName;
							var a = document.createElement('a'),
						    ev = document.createEvent("MouseEvents");
							a.href = url;
							a.download = url.slice(url.lastIndexOf('/')+1);
							ev.initMouseEvent("click", true, false, self, 0, 0, 0, 0, 0,
								false, false, false, false, 0, null);
							a.dispatchEvent(ev);
							var result = {};
							dispatcher.fire(_this.GET_LIBRARY_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.GET_LIBRARY_ERROR, jqXHR, res, error);
						}})
		
				};
				
				this.init = function() {

				};

				this.on = function(eventName, callback) {
					dispatcher.on(eventName, callback);
				};
				this.off = function (eventName, callback) {
					dispatcher.off(eventName, callback);
				};

				this.fire = function(eventName, eventInfo) {
					dispatcher.fire(eventName, eventInfo);
				};
			}

			return new ToolsHandler();
		}());

		return ToolsHandler;
});