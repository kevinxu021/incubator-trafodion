// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015-2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var WorkloadsHandler = ( function() {
			var xhrs = [];

			function WorkloadsHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;
				this.FETCH_REPO_SUCCESS = 'fetchRespoSuccess';
				this.FETCH_REPO_ERROR = 'fetchRespoError';
				this.FETCH_ACTIVE_SUCCESS = 'fetchActiveSuccess';
				this.FETCH_ACTIVE_ERROR = 'fetchActiveError';
				this.FETCH_REPO_QUERY_DETAIL_SUCCESS = 'fetchRepoQDetailSuccess';
				this.FETCH_REPO_QUERY_DETAIL_ERROR = 'fetchRepoQDetailError';
				this.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS = 'fetchActiveQDetailSuccess';
				this.FETCH_ACTIVE_QUERY_DETAIL_ERROR = 'fetchActiveQDetailError';
				this.CANCEL_QUERY_SUCCESS = 'CancelQuerySuccess';
				this.CANCEL_QUERY_ERROR = 'CancelQueryError';
				
				this.FETCH_PROFILES_SUCCESS = 'FETCH_PROFILES_SUCCESS';
				this.FETCH_PROFILES_ERROR = 'FETCH_PROFILES_ERROR';
				this.ADDALTER_PROFILE_SUCCESS = 'ADDALTER_PROFILE_SUCCESS';
				this.ADDALTER_PROFILE_ERROR = 'ADDALTER_PROFILE_ERROR';
				this.DELETE_PROFILE_SUCCESS = 'DELETE_PROFILE_SUCCESS';
				this.DELETE_PROFILE_ERROR = 'DELETE_PROFILE_ERROR';
				
				this.FETCH_SLAS_SUCCESS = 'FETCH_SLAS_SUCCESS';
				this.FETCH_SLAS_ERROR = 'FETCH_SLAS_ERROR';
				this.ADDALTER_SLA_SUCCESS = 'ADDALTER_SLA_SUCCESS';
				this.ADDALTER_SLA_ERROR = 'ADDALTER_SLA_ERROR';
				this.DELETE_SLA_SUCCESS = 'DELETE_SLA_SUCCESS';
				this.DELETE_SLA_ERROR = 'DELETE_SLA_ERROR';
				
				this.FETCH_MAPPINGS_SUCCESS = 'FETCH_MAPPINGS_SUCCESS';
				this.FETCH_MAPPINGS_ERROR = 'FETCH_MAPPINGS_ERROR';		
				this.ADDALTER_MAPPINGS_SUCCESS = 'ADDALTER_MAPPINGS_SUCCESS';
				this.ADDALTER_MAPPINGS_ERROR = 'ADDALTER_MAPPINGS_ERROR';
				this.DELETE_MAPPING_SUCCESS = 'DELETE_MAPPING_SUCCESS';
				this.DELETE_MAPPING_ERROR = 'DELETE_MAPPING_ERROR';
				
				this.FETCH_TOPN_MAX_MEM_USED_SUCCESS = 'FETCH_TOPN_MAX_MEM_USED_SUCCESS';
				this.FETCH_TOPN_MAX_MEM_USED_ERROR = 'FETCH_TOPN_MAX_MEM_USED_ERROR';
				this.FETCH_TOPN_CPU_TIME_SUCCESS = 'FETCH_TOPN_CPU_TIME_SUCCESS';
				this.FETCH_TOPN_CPU_TIME_ERROR = 'FETCH_TOPN_CPU_TIME_ERROR';
				this.FETCH_TOPN_Rumtime_SUCCESS = 'FETCH_TOPN_Rumtime_SUCCESS';
				this.FETCH_TOPN_Rumtime_ERROR = 'FETCH_TOPN_Rumtime_ERROR';
				this.FETCH_TOPN_DiskIO_SUCCESS = 'FETCH_TOPN_DiskIO_SUCCESS';
				this.FETCH_TOPN_DiskIO_ERROR = 'FETCH_TOPN_DiskIO_SUCCESS';
				
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchQueriesInRepository = function(params){
					var xhr = xhrs["repo_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["repo_list"] = $.ajax({
						url: 'resources/workloads/repo',
						type:'POST',
						data: JSON.stringify(params),
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_REPO_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_REPO_ERROR, jqXHR, res, error);
						}
					});
				}; 

				this.fetchRepositoryQueryDetail = function(queryID){

					var xhr = xhrs["repo_detail"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["repo_detail"] = $.ajax({
						cache: false,
						url: 'resources/workloads/repo/detail?queryID=' + queryID,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_REPO_QUERY_DETAIL_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_REPO_QUERY_DETAIL_ERROR, jqXHR, res, error);
						}
					});
				};

				this.cancelQuery = function(queryID, requestor){

					var xhr = xhrs["cancel_query"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["cancel_query"] = $.ajax({
						cache: false,
						url: 'resources/workloads/cancel?queryID=' + queryID,
						type:'DELETE',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.data = data;
							result.queryID = queryID;
							result.requestor = requestor;
							
							dispatcher.fire(_this.CANCEL_QUERY_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.queryID = queryID;
							jqXHR.requestor = requestor;
							dispatcher.fire(_this.CANCEL_QUERY_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchActiveQueries = function(){

					var xhr = xhrs["active_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["active_list"] = $.ajax({
						cache: false,
						url: 'resources/workloads/active',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_ACTIVE_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_ACTIVE_ERROR, jqXHR, res, error);
						}
					});
				};

				this.fetchActiveQueryDetail = function(queryID){

					var xhr = xhrs["active_detail"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["active_detail"] = $.ajax({
						cache: false,
						url: 'resources/workloads/active/detailnew?queryID=' + queryID,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_ACTIVE_QUERY_DETAIL_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchTopMemUsed = function(startTime,endTime){
					var xhr = xhrs["topN_mem_used"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["topN_mem_used"] = $.ajax({
						cache: false,
						url: 'resources/workloads/repo/top_mem_used/'+startTime+"/"+endTime,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_TOPN_MAX_MEM_USED_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_TOPN_MAX_MEM_USED_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchTopCPUTime = function(startTime,endTime){
					var xhr = xhrs["topcpu"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["topcpu"] = $.ajax({
						cache: false,
						url: 'resources/workloads/repo/topcpu/'+startTime+"/"+endTime,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_TOPN_CPU_TIME_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_TOPN_CPU_TIME_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchTopRuntime = function(startTime,endTime){
					var xhr = xhrs["topruntime"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["topruntime"] = $.ajax({
						cache: false,
						url: 'resources/workloads/repo/topruntime/'+startTime+"/"+endTime,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_TOPN_Rumtime_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_TOPN_Rumtime_ERROR, jqXHR, res, error);
						}
					});
				};

				this.fetchTopDiskIO = function(startTime,endTime){
					var xhr = xhrs["topdiskio"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["topdiskio"] = $.ajax({
						cache: false,
						url: 'resources/workloads/repo/topdiskio/'+startTime+"/"+endTime,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_TOPN_DiskIO_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_TOPN_DiskIO_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchProfiles = function(async){
					var xhr = xhrs["profiles_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["profiles_list"] = $.ajax({
						cache: false,
						url: 'resources/workloads/profiles',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						async:(async != null && async == true),
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_PROFILES_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_PROFILES_ERROR, jqXHR, res, error);
						}
					});
				}; 
				
				this.addAlterProfile = function(param){
					var xhr = xhrs["addAlterProfile"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["addAlterProfile"] = $.ajax({
						cache: false,
		        	    url:'resources/workloads/profile',
		        	    type:'POST',
		        	    data: JSON.stringify(param),
		        	    dataType:"json",
		        	    contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success:  function(data){
							dispatcher.fire(_this.ADDALTER_PROFILE_SUCCESS, data);
						},
		        	    error:function(jqXHR, res, error){
		        	    	dispatcher.fire(_this.ADDALTER_PROFILE_ERROR, jqXHR, res, error);
		        	    }
		        	});
				};
				
				this.deleteProfile = function(profile){
					$.ajax({
						cache: false,
						url: 'resources/workloads/profile?profile=' + profile,
						type:'DELETE',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.profile = profile;
							dispatcher.fire(_this.DELETE_PROFILE_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.profile = profile;
							dispatcher.fire(_this.DELETE_PROFILE_ERROR, jqXHR, res, error);
						}
					});
				}; 
				
				this.fetchSLAs = function(params){
					var xhr = xhrs["slas_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["slas_list"] = $.ajax({
						cache: false,
						url: 'resources/workloads/slas',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_SLAS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_SLAS_ERROR, jqXHR, res, error);
						}
					});
				}; 
				
				this.addAlterSLA = function(param){
					var xhr = xhrs["addAlterSLA"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["addAlterSLA"] = $.ajax({
						cache: false,
		        	    url:'resources/workloads/sla',
		        	    type:'POST',
		        	    data: JSON.stringify(param),
		        	    dataType:"json",
		        	    contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success:  function(data){
							dispatcher.fire(_this.ADDALTER_SLA_SUCCESS, data);
						},
		        	    error:function(jqXHR, res, error){
		        	    	dispatcher.fire(_this.ADDALTER_SLA_ERROR, jqXHR, res, error);
		        	    }
		        	});
				};
				
				this.deleteSLA = function(sla){

					$.ajax({
						cache: false,
						url: 'resources/workloads/sla?sla=' + sla,
						type:'DELETE',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.sla = sla;
							dispatcher.fire(_this.DELETE_SLA_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.sla = sla;
							dispatcher.fire(_this.DELETE_SLA_ERROR, jqXHR, res, error);
						}
					});
				}; 
				
				this.fetchMappings = function(params){
					var xhr = xhrs["mappings_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["mappings_list"] = $.ajax({
						cache: false,
						url: 'resources/workloads/mappings',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_MAPPINGS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_MAPPINGS_ERROR, jqXHR, res, error);
						}
					});
				}; 
				
				this.addAlterMapping = function(param){
					var xhr = xhrs["addAlterMapping"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["addAlterMapping"] = $.ajax({
						cache: false,
		        	    url:'resources/workloads/mapping',
		        	    type:'POST',
		        	    data: JSON.stringify(param),
		        	    dataType:"json",
		        	    contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success:  function(data){
							dispatcher.fire(_this.ADDALTER_MAPPING_SUCCESS, data);
						},
		        	    error:function(jqXHR, res, error){
		        	    	dispatcher.fire(_this.ADDALTER_MAPPING_ERROR, jqXHR, res, error);
		        	    }
		        	});
				};
				
				this.deleteMapping = function(mapping){
					$.ajax({
						cache: false,
						url: 'resources/workloads/mapping?mapping=' + mapping,
						type:'DELETE',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.mapping = mapping;
							dispatcher.fire(_this.DELETE_MAPPING_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.sla = mapping;
							dispatcher.fire(_this.DELETE_MAPPING_ERROR, jqXHR, res, error);
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

			return new WorkloadsHandler();
		}());

		return WorkloadsHandler;
});
