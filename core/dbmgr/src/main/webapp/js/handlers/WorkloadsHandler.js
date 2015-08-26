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
        	this.FETCHREPOS_SUCCESS = 'fetchResposSuccess';
        	this.FETCHREPOS_ERROR = 'fetchResposError';
        	this.FETCH_REPO_QUERY_DETAIL_SUCCESS = 'fetchRepoQDetailSuccess';
        	this.FETCH_REPO_QUERY_DETAIL_ERROR = 'fetchRepoQDetailError';
        	
            /**
             * call memory skew
             */
        	this.fetchQueriesInRepository = function(params){
        		$.ajax({
    				url: 'resources/workloads/repo',
    				type:'POST',
    				data: JSON.stringify(params),
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire(_this.FETCHREPOS_SUCCESS, data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire(_this.FETCHREPOS_ERROR, jqXHR, res, error);
    				}
    			});
            }; 
            
            this.fetchRepositoryQueryDetail = function(queryID){

        		$.ajax({
    				url: 'resources/workloads/repo/detail?queryID=' + queryID,
    				type:'GET',
    				dataType:"json",
    				contentType: "application/json;",
    				success: function(data){
    					dispatcher.fire(_this.FETCH_REPO_QUERY_DETAIL_SUCCESS, data);
    				},
    				error:function(jqXHR, res, error){
    					dispatcher.fire(_this.FETCH_REPO_QUERY_DETAIL_ERROR, jqXHR, res, error);
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