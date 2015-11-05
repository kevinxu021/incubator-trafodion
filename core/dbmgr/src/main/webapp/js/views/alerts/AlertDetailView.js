//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/alert_detail.html',
        'jquery',
        'handlers/ServerHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        ], function (BaseView, AlertDetailT, $, serverHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	REFRESH_MENU = '#refreshAction',
	ERROR_CONTAINER = '#alert-error-text',
	ALERT_TEXT = '#alert-details-text',
	RESULT_CONTAINER = '#alert-detail-container',
	ALERT_ID = '#alert-id',
	ALERT_NAME = '#alert-name',
	ALERT_STATUS = '#alert-status',
	ALERT_SEVERITY = '#alert-severity',
	ALERT_TIME = '#alert-time',
	ALERT_UPDATE_FORM = '#alert-action-form',
	UPDATE_ACTION = '#updateAction',
	UPDATE_DIALOG = '#alertUpdateDialog',
	DIALOG_APPLY_BUTTON = "#updateAlertApplyButton",
	DIALOG_ALERT_ID = '#action-alert-id';

	var _that = null;
	var alertID = null;
	var validator = null;

	var AlertDetailView = BaseView.extend({
		template:  _.template(AlertDetailT),

		doInit: function (args){
			_that = this;
			$(ALERT_ID).val(args);
			alertID = args;
			$(DIALOG_ALERT_ID).val(args);
			serverHandler.on(serverHandler.FETCH_ALERT_DETAIL_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCH_ALERT_DETAIL_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.ALERT_UPDATE_SUCCESS, this.updateSuccess);
			serverHandler.on(serverHandler.ALERT_UPDATE_ERROR, this.updateFailure);

			$(REFRESH_MENU).on('click', this.fetchAlertDetail);
			$(UPDATE_ACTION).on('click', this.updateAlert);
			$(DIALOG_APPLY_BUTTON).on('click', this.dialogApplyClicked);

			validator = $(ALERT_UPDATE_FORM).validate({
				rules: {
					"alert-update-message": { required: true }
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

			$(ALERT_UPDATE_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(DIALOG_APPLY_BUTTON).attr('disabled', false);
				} else {
					$(DIALOG_APPLY_BUTTON).attr('disabled', true);
				}
			});

			$(UPDATE_DIALOG).on('hide.bs.modal', function(e){

			});

			$(UPDATE_DIALOG).on('show.bs.modal', function(e){
				var alertState = $(ALERT_STATUS).val();
				if(alertState == 'Acknowledged'){
					$('#update-ack').prop("disabled", true);
					$('#update-close').prop("disabled", false);
					$('#update-close').prop('checked', true);
				}else{
					$('#update-ack').prop("disabled", false);
					$('#update-ack').prop('checked', true);
					$('#update-close').prop("disabled", true);
				}
				$('#alert-update-notify').prop('checked', true);

			});

			this.fetchAlertDetail();

		},
		doResume: function(args){
			$(ALERT_ID).val(args);
			$(DIALOG_ALERT_ID).val(args);
			alertID = args;
			serverHandler.on(serverHandler.FETCH_ALERT_DETAIL_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCH_ALERT_DETAIL_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.ALERT_UPDATE_SUCCESS, this.updateSuccess);
			serverHandler.on(serverHandler.ALERT_UPDATE_ERROR, this.updateFailure);

			$(REFRESH_MENU).on('click', this.fetchAlertDetail);
			$(UPDATE_ACTION).on('click', this.updateAlert);
			$(DIALOG_APPLY_BUTTON).on('click', this.dialogApplyClicked);

			this.fetchAlertDetail();
		},
		doPause: function(){
			serverHandler.off(serverHandler.FETCH_ALERT_DETAIL_SUCCESS, this.displayResults);
			serverHandler.off(serverHandler.FETCH_ALERT_DETAIL_ERROR, this.showErrorMessage);
			serverHandler.off(serverHandler.ALERT_UPDATE_SUCCESS, this.updateSuccess);
			serverHandler.off(serverHandler.ALERT_UPDATE_ERROR, this.updateFailure);

			$(REFRESH_MENU).off('click', this.fetchAlertDetail);
			$(UPDATE_ACTION).off('click', this.updateAlert);
			$(DIALOG_APPLY_BUTTON).off('click', this.dialogApplyClicked);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},

		updateAlert: function(){
			$(UPDATE_DIALOG).modal('show');
		},
		dialogApplyClicked: function(){
			if($(ALERT_UPDATE_FORM).valid()){

			}else{
				return;
			}
			var params = {};
			params.alertID = alertID;
			params.action = $('#update-ack').prop('checked') ? 'ack' : 'close';
			params.notify = $('#alert-update-notify').prop('checked');
			params.message = $('#alert-update-message').val();

			serverHandler.updateAlert(params);
			$(UPDATE_DIALOG).modal('hide')
		},
		updateSuccess: function(data){
			_this.fetchAlertDetail();
		},
		updateFailure: function(jqXHR){
			alert(jqXHR.responseText);
		},
		fetchAlertDetail: function(){
			_that.showLoading();
			var params = {};
			params.alertID = alertID;
			serverHandler.fetchAlertDetail(params);
		},

		displayResults: function (result){
			_that.hideLoading();
			var alertDetail = result[alertID];
			var state = 'UnKnown';
			if(alertDetail && alertDetail.NeedAck == true){
				state = 'Un-Acknowledged';
			}
			if(alertDetail && alertDetail.NeedAck == false){
				state = 'Acknowledged';
			}
			$(ERROR_CONTAINER).hide();
			$(RESULT_CONTAINER).show();
			$(ALERT_NAME).val(alertDetail.AlertName);
			$(ALERT_STATUS).val(state);
			$(RESULT_CONTAINER).html(result[alertID].Body);

			var alertHistory = alertDetail.History;
			if(alertHistory && alertHistory.length > 0){
				$(ALERT_SEVERITY).val(alertHistory[alertHistory.length -1].Status);
				var time = moment(alertHistory[alertHistory.length -1].Time);
				$(ALERT_TIME).val(time.tz(common.serverTimeZone).format('YYYY-MM-DD HH:mm:ss'));
			}
		},

		showErrorMessage: function (jqXHR) {
			_that.hideLoading();
			$(RESULT_CONTAINER).hide();
			$(ERROR_CONTAINER).show();
			if (jqXHR.responseText) {
				$(ERROR_CONTAINER).text(jqXHR.responseText);
			}
		}  
	});


	return AlertDetailView;
});
