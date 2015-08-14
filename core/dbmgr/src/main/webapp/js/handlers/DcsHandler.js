define(['handlers/EventDispatcher'],
function(EventDispatcher) {"use strict";

    var DcsHandler = ( function() {
    	
        /**
         * @constructor
         * @type {DatabaseHandler}
         */
        function DcsHandler() {
        	var dispatcher = new EventDispatcher();
        	var _this = this;

        	this.FETCHDCS_SUCCESS = 'fetchDcsServersSuccess';
        	this.FETCHDCS_ERROR = 'fetchDcsServersError';
        	
            /**
             * call memory skew
             */
        	this.fetchDcsServers = function(){
        		$.ajax({
    				url: 'resources/dcs/connections',
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire(_this.FETCHDCS_SUCCESS, data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire(_this.FETCHDCS_ERROR, jqXHR, res, error);
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

        return new DcsHandler();
    }());

    return DcsHandler;
});