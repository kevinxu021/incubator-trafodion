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

				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.createLibrary = function(file, fileName, filePart, fileSize, schemaName, libraryName, sflag, eflag){
					_this.fileSize=fileSize;
					var fd = new FormData();
					fd.append("file", file);
					fd.append("fileName", fileName);
					fd.append("filePart", filePart);
					fd.append("schemaName", schemaName);
					fd.append("libraryName", libraryName);
					fd.append("startFlag", sflag);
					fd.append("endFlag", eflag);
					
					var xhr = xhrs["create_library"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
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
							dispatcher.fire(_this.CREATE_LIBRARY_SUCCESS);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.CREATE_LIBRARY_ERROR, jqXHR, res, error);
						}
					});
				
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