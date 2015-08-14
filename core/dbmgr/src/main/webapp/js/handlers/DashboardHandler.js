define(['handlers/EventDispatcher'],
function(EventDispatcher) {"use strict";

    var DashboardHandler = ( function() {
        /**
         * @constructor
         * @type {DashboardHandler}
         */
        function DashboardHandler() {
        	var dispatcher = new EventDispatcher();
        	  
            /**
             * call memory skew
             */
        	this.fetchCPUData = function(uri){
        		$.ajax({
    				url: 'resources/metrics/cpu',
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire("fetchCPUDataSuccess", data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire("fetchCPUDataError", jqXHR, res, error);
    				}
    			});
            };           
                                  
        	this.fetchDiskReads = function(uri){
        		$.ajax({
    				url: 'resources/metrics/diskreads',
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire("fetchDiskReadsSuccess", data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire("fetchDiskReadsError", jqXHR, res, error);
    				}
    			});
            };
            
        	this.fetchDiskWrites = function(uri){
        		$.ajax({
    				url: 'resources/metrics/diskwrites',
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire("fetchDiskWritesSuccess", data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire("fetchDiskWritesError", jqXHR, res, error);
    				}
    			});
            };
            
        	this.fetchGetOps = function(uri){
        		$.ajax({
    				url: 'resources/metrics/getops',
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire("fetchGetOpsSuccess", data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire("fetchGetOpsError", jqXHR, res, error);
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

        return new DashboardHandler();
    }());

    return DashboardHandler;
});