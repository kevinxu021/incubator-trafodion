define(['handlers/EventDispatcher'],
function(EventDispatcher) {"use strict";

    var WorkloadsHandler = ( function() {
        /**
         * @constructor
         * @type {WorkloadsHandler}
         */
        function WorkloadsHandler() {
        	var dispatcher = new EventDispatcher();
        	var _this = this;
        	this.FETCHWORKLOADS_SUCCESS = 'fetchWorkloadsSuccess';
        	this.FETCHWORKLOADS_ERROR = 'fetchWorkloadsError';
        	
            /**
             * call memory skew
             */
        	this.fetchWorkloads = function(){
        		$.ajax({
    				url: 'resources/workloads/list',
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire(_this.FETCHWORKLOADS_SUCCESS, data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire(_this.FETCHWORKLOADS_ERROR, jqXHR, res, error);
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

        return new WorkloadsHandler();
    }());

    return WorkloadsHandler;
});