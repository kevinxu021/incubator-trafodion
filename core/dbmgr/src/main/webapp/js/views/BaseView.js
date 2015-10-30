// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'jquery',
        'underscore',
        'backbone',
        'common',
        'handlers/ServerHandler',
        'jqueryui',
        ], function ($, _, Backbone, common, serverHandler) {
	'use strict';
    var _this = null;
    
	var BaseView = Backbone.View.extend({

		el: $('#wrapper'),

		initialized: false,

		pageWrapper: null,

		/*initialize: function () {

		},*/
    	refreshTimer: (function() {
			var _interval;
			var _intervalId;
			var _refreshTimer = {};
			
			_refreshTimer.set = function(newInterval) {
				if(newInterval){
					if(typeof newInterval === 'string'){
						var value = parseFloat(newInterval);
						if(!isNaN(value)) {
							_interval = value * 60 * 1000;
						}
					}else{
						_interval = newInterval * 60 * 1000;
					}
					
					if(_interval != 0){
						_refreshTimer.start();
					}else{
						_refreshTimer.stop();
					}
				}else{
					_refreshTimer.stop();
				}

			};
			
			_refreshTimer.get = function() {
				return _interval;
			};
			
			_refreshTimer.beep = function() {
				// console.log("========beep==========");
				if(_this.onTimerBeeped){
					_this.onTimerBeeped();
				}
			};
			
			_refreshTimer.start = function() {
				_refreshTimer.stop();
				if (_interval) {
					_intervalId = window.setInterval(_refreshTimer.beep, _interval);
				}
			};
			
			_refreshTimer.stop = function() {
				if (_intervalId) {
					window.clearInterval(_intervalId);
				}
			};
			
			return _refreshTimer;
		}()),
		
		initialize: function(options) { 
			_.bindAll(this, 'beforeRender', 'render', 'afterRender'); 
			_this = this; 
			
			this.render = _.wrap(this.render, function(render, args) { 
				_this.beforeRender(); 
				render(args); 
				_this.afterRender(); 
				return _this; 
			}); 
		}, 

		beforeRender: function() { 
		}, 

		render: function (args) {
			if(this.pageWrapper == null){
				this.$el.html(this.template);
				if(common.serverTimeZone == null){
					serverHandler.loadServerConfig();
				}
				this.init(args);
			}else
			{
				//this.pause();
				this.$el.empty().append(this.pageWrapper);
				this.resume(args);
			}
			return this;        	
		},
		
		afterRender: function() { 
		},

		init: function(args){
			if(this.doInit){
				this.doInit(args);
			}
		},
		resume: function(args){
			if(this.doResume){
				this.doResume(args);
			}
		},
		pause: function() {
			if(this.doPause){
				this.doPause();
			}
		},

		remove: function(){
			_this.refreshTimer.stop();
			this.pause();
			var childElement = $(this.$el[0]).find('#page-wrapper');
			if(childElement && childElement.length > 0)
				this.pageWrapper = childElement.detach();

		},
	});

	return BaseView;
});
