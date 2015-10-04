//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'jquery',
        'underscore',
        'backbone'
        ], function ($, _, Backbone) {
	'use strict';
	var _this = null;
	var REFRESH_INTERVAL = '#refreshInterval';
	var _refreshTimer = null;
	
	var RefreshTimerView = Backbone.View.extend({

		eventAgg: null,
		
		events: {
			TIMER_BEEPED: 'timer_beep',
			INTERVAL_CHANGED: 'interval_changed'
		},
		
		setRefreshInterval: function(interval){
			_this.refreshTimer.set(interval);
		},
		
		stopTimer: function(){
			_this.refreshTimer.stop();
		},

		startTimer: function(){
			_this.refreshTimer.start();
		},
		
		restart: function(){
			_this.refreshTimer.stop();
			_this.refreshTimer.start();
		},
		refreshTimer: (function() {
			var _interval;
			var _intervalId;
			var _refreshTimer = {};

			_refreshTimer.set = function(newInterval) {
				_refreshTimer.stop();
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
					}
				}else{
					_interval = null;
				}
			};

			_refreshTimer.get = function() {
				return _interval;
			};

			_refreshTimer.beep = function() {
				_this.eventAgg.trigger(_this.events.TIMER_BEEPED)
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
			
			_refreshTimer.reset = function() {
	        	var interval = $(REFRESH_INTERVAL).val();
	    		_refreshTimer.set(interval);
			};

			_refreshTimer.bindEvent = function() {
				if($(REFRESH_INTERVAL).length > 0)
					$(REFRESH_INTERVAL).on('change', _refreshTimer.reset);
				_refreshTimer.reset();
			};

			_refreshTimer.unBindEvent = function() {
				if($(REFRESH_INTERVAL).length > 0)
					$(REFRESH_INTERVAL).off('change', _refreshTimer.reset);
				_refreshTimer.stop();
			};

			return _refreshTimer;
		}()),

				
		initialize: function(options) { 
			this.eventAgg = _.extend({}, Backbone.Events);
			//_this = this;
			//_refreshTimer = this.refreshTimer;
		}, 

		init: function(){
			_this.refreshTimer.bindEvent();
		},

		pause: function(){
			_this.refreshTimer.unBindEvent();
		},

		resume: function(){
			_this.refreshTimer.bindEvent();
		}
	});
	
	_this = new RefreshTimerView();
	return _this;
});
