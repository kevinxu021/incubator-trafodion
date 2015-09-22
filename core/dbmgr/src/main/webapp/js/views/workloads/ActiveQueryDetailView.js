// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/active_query_detail.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools'
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common) {
	'use strict';
    var LOADING_SELECTOR = "#loadingImg",
  		REFRESH_MENU = '#refreshAction',
  		QCANCEL_MENU = '#cancelAction';

    var _that = null;
    var queryID = null;
	var ActiveQueryDetailView = BaseView.extend({
		template:  _.template(WorkloadsT),

		init: function (args){
			_that = this;
			$('#query-id').val(args);
			queryID = args;
			
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			
			$(REFRESH_MENU).on('click', this.fetchActiveQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			this.fetchActiveQueryDetail();
			
		},
		resume: function(args){
			$('#query-id').val(args);
			queryID = args;
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).on('click', this.fetchActiveQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			this.fetchActiveQueryDetail();
		},
		pause: function(){
			wHandler.off(wHandler.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_ACTIVE_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.off(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.off(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).off('click', this.fetchActiveQueryDetail);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
		},
        showLoading: function(){
        	$(LOADING_SELECTOR).show();
        },

        hideLoading: function () {
        	$(LOADING_SELECTOR).hide();
        },
        cancelQuery: function(){
        	wHandler.cancelQuery(queryID);
        },
        cancelQuerySuccess:function(){
        	alert('The cancel query request has been submitted');
        },
        cancelQueryError:function(jqXHR){
        	alert(jqXHR.responseText);
        },
        fetchActiveQueryDetail: function(){
			_that.showLoading();
			//$(ERROR_CONTAINER).hide();
			wHandler.fetchActiveQueryDetail(queryID);
		},

		displayResults: function (result){
			_that.hideLoading();
			$('#query-statistics').text(result.join("\n"));
		},

        showErrorMessage: function (jqXHR) {
        	_that.hideLoading();
        	/*$(RESULT_CONTAINER).hide();
        	$(ERROR_CONTAINER).show();
        	if (jqXHR.responseText) {
        		$(ERROR_CONTAINER).text(jqXHR.responseText);
        	}*/
        }  

	});


	return ActiveQueryDetailView;
});
