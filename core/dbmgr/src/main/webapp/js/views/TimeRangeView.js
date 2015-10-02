//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'jquery',
        'underscore',
        'backbone',
        'common',
        'moment'
        ], function ($, _, Backbone, common, moment) {
	'use strict';
	var _this = null;

	var _timeRangeControl = null;
	var validator = null;

	var DATE_FORMAT = 'YYYY-MM-DD HH:mm:ss',
		DATE_FORMAT_ZONE = DATE_FORMAT + ' z',
		START_TIME_PICKER = '#startdatetimepicker',
		END_TIME_PICKER = '#enddatetimepicker';
	
	 var TIME_RANGE = '#timeRange',
	 	TIME_RANGE_LABEL = '#timeRangeLabel',
	 	FILTER_DIALOG = '#filterDialog',
 		FILTER_FORM = '#filter-form',
		FILTER_APPLY_BUTTON = "#filterApplyButton",
		FILTER_START_TIME = '#filter-start-time',
		FILTER_END_TIME = '#filter-end-time';
	
	var TimeRangeView = Backbone.View.extend({

		eventAgg: null,
		
		events: {
			TIME_RANGE_CHANGED: 'time_range_changed'
		},
		setTimeRange: function(range){
			_timeRangeControl.set(range);
		},
		updateTimeRangeLabel: function(){
			$(TIME_RANGE_LABEL).text('Time Range : ' +  $(START_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE) + ' - ' +  $(END_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE));
		},
		openCustomTimeDialog: function(){
			$(FILTER_DIALOG).modal('show');
		},
		filterApplyClicked: function(){
			if(!$(FILTER_FORM).valid()){
				return;
			}
			_this.updateTimeRangeLabel();

			var startTime = $(START_TIME_PICKER).data("DateTimePicker").date();
			var endTime = $(END_TIME_PICKER).data("DateTimePicker").date();

			var param = {};
			param.startTime = startTime.format(DATE_FORMAT);
			param.endTime = endTime.format(DATE_FORMAT);

			$(FILTER_DIALOG).modal('hide');
			_this.eventAgg.trigger(_this.events.TIME_RANGE_CHANGED);
		},        
		timeRangeChanged: function(){
			var sel = $(this).val();
			$(this).blur();
			if(sel == '0'){
				_this.openCustomTimeDialog();
			}else{
				_this.eventAgg.trigger(_this.events.TIME_RANGE_CHANGED);
				_this.updateFilter();
			}
		},
		updateFilter: function() {
			var sel = $(TIME_RANGE).val();
			switch(sel){
			case "1":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;
			case "6":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;
			case "24":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;
			case "128":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'week'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;			
			}
			_this.updateTimeRangeLabel();
		},
		timeRangeControl: (function() {
			var _interval;
			var _intervalId;
			var _timeRangeControl = {};

			_timeRangeControl.set = function(newRange) {
				$(TIME_RANGE).val(newRange);
			};

			_timeRangeControl.get = function() {
				return $(TIME_RANGE).val();
			};

			_timeRangeControl.bindEvent = function() {
				if($(TIME_RANGE).length > 0){
					$(TIME_RANGE).on('change', _this.timeRangeChanged);
				}
				if($(FILTER_APPLY_BUTTON).length > 0){
					$(FILTER_APPLY_BUTTON).on('click', _this.filterApplyClicked);
				}
			};

			_timeRangeControl.unBindEvent = function() {
				if($(TIME_RANGE).length > 0)
					$(TIME_RANGE).off('change', _this.timeRangeChanged);
				if($(FILTER_APPLY_BUTTON).length > 0){
					$(FILTER_APPLY_BUTTON).off('click', _this.filterApplyClicked);
				}
			};

			return _timeRangeControl;
		}()),
		
		initialize: function(options) { 
			this.eventAgg = _.extend({}, Backbone.Events);
			_this = this;
			_timeRangeControl = this.timeRangeControl;
		}, 

		init: function(){
			validator = $(FILTER_FORM).validate({
				rules: {
					"filter-start-time": { required: true },
					"filter-end-time": { required: true }
				},
				highlight: function(element) {
					$(element).closest('.form-group').addClass('has-error');
				},
				unhighlight: function(element) {
					$(element).closest('.form-group').removeClass('has-error');
				},
				errorElement: 'span',
				errorClass: 'help-block',
				errorPlacement: function(error, element) {
					if(element.parent('.input-group').length) {
						error.insertAfter(element.parent());
					} else {
						error.insertAfter(element);
					}
				}
			});
			$(TIME_RANGE).val(-1);

			$(FILTER_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(FILTER_APPLY_BUTTON).attr('disabled', false);
				} else {
					$(FILTER_APPLY_BUTTON).attr('disabled', true);
				}
			});

			$(START_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE});
			$(END_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE});
			$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
			$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));

			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				$('#filter-start-time').prop("disabled", false);
				$('#filter-end-time').prop("disabled", false);
			});

			$(TIME_RANGE).focusin(function(){
				$(this).val(-1);
			});

			_timeRangeControl.bindEvent();
		},

		pause: function(){
			_timeRangeControl.unBindEvent();
		},

		resume: function(){
			_timeRangeControl.bindEvents();
		}
	});
	
	_this = new TimeRangeView();
	return _this;
});
