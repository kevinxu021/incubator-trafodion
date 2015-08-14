define(['handlers/EventDispatcher'],
function(EventDispatcher) {"use strict";

    var DatabaseHandler = ( function() {
        /**
         * @constructor
         * @type {DatabaseHandler}
         */
        function DatabaseHandler() {
        	var dispatcher = new EventDispatcher();
        	  
            /**
             * call memory skew
             */
        	this.fetchSchemas = function(uri){
        		$.ajax({
    				url: uri,
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire("fetchSchemasSuccess", data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire("fetchSchemasError", jqXHR, res, error);
    				}
    			});
            };           
                                  
            /**
             * @public
             */
            this.init = function() {
 
            };

            /**
             * @public
             * Add a listener for a specified event.
             * @param {string} eventName The name of the event.
             * @param {function(...)}
             */
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