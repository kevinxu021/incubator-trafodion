// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/about.html',
        'jquery',
        'model/Localizer',
        'moment',
        'common',
        'views/RefreshTimerView',
        'jqueryui',
        'jqueryvalidate'
        ], function (BaseView, aboutT, $, localizer, moment, common, refreshTimerView) {
	'use strict';

	var AboutView = BaseView.extend({
		template:  _.template(aboutT),

		doInit: function (){}

	});


	return AboutView;
});