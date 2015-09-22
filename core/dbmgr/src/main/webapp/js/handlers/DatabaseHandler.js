define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var DatabaseHandler = ( function() {

			function DatabaseHandler() {
				var dispatcher = new EventDispatcher();

				var _this = this;
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchSchemas = function(uri){
					$.ajax({
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire("fetchSchemasSuccess", data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire("fetchSchemasError", jqXHR, res, error);
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

			return new DatabaseHandler();
		}());

		return DatabaseHandler;
});