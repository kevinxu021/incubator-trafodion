// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/about.html',
        'jquery',
        'handlers/ServerHandler'
          ], function (BaseView, aboutT, $, serverHandler) {
	'use strict';

	var SYSTEM_VERSION = '#system-version',
		DBMGR_VERSION = '#dbmgr-version',
		SPINNER = '#loadingImg',
		RESULT_CONTAINER = '#version-container',
		ERROR_CONTAINER= '#errorText';

	var _that = null;

	var AboutView = BaseView.extend({
		template:  _.template(aboutT),

		doInit: function (){
			serverHandler.on(serverHandler.FETCH_VERSION_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCH_VERSION_ERROR, this.showErrorMessage);	
			$(SPINNER).show();
			serverHandler.fetchServerInfo();
		},
		displayResults: function(data){
			$(SPINNER).hide();
			$(ERROR_CONTAINER).hide();
			$(RESULT_CONTAINER).show();
			$(SYSTEM_VERSION).val(data.SYSTEM_VERSION);
			$(DBMGR_VERSION).val(data.DBMGR_VERSION);
		},
		showErrorMessage: function (jqXHR) {
			$(SPINNER).hide();
			$(RESULT_CONTAINER).hide();
			$(ERROR_CONTAINER).show();
			if (jqXHR.responseText) {
				$(ERROR_CONTAINER).text(jqXHR.responseText);
			}
		}  

	});


	return AboutView;
});