define(['handlers/EventDispatcher'],
function(EventDispatcher) {"use strict";

    var LogsHandler = ( function() {
        /**
         * @constructor
         * @type {LogsHandler}
         */
        function LogsHandler() {
        	var dispatcher = new EventDispatcher();
        	var _this = this;

        	this.FETCHLOGS_SUCCESS = 'fetchLogsSuccess';
        	this.FETCHLOGS_ERROR = 'fetchLogsError';
        	
            /**
             * call memory skew
             */
        	this.fetchLogs = function(params){
        		var request = $.ajax({
    				url: 'resources/logs/list',
    				type:'POST',
    				data: JSON.stringify(params),
    				dataType:"json",
    				contentType: "application/json",
    				success: function(result){
    					dispatcher.fire(_this.FETCHLOGS_SUCCESS, result);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire(_this.FETCHLOGS_ERROR, jqXHR, res, error);
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

        return new LogsHandler();
    }());

    return LogsHandler;
});