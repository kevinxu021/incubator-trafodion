//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workload_configuration.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'handlers/ServerHandler',
        'moment',
        'common',
        'views/workloads/WorkloadProfileConfigurationView',
        'views/workloads/WorkloadSLAConfigurationView',
        'views/workloads/WorkloadMappingConfigurationView',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datatables.net-buttons',
        'datatables.net-select',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, WorkloadsT, $, wHandler, sHandler, moment, common, 
        		WorkloadProfileConfigurationView, WorkloadSLAConfigurationView, WorkloadMappingConfigurationView) {
	'use strict';

	var _this = null;
	var workloadProfileConfigurationView = null;
	var workloadSLAConfigurationView = null;
	var workloadMappingConfigurationView = null;
	
	var CONFIG_DETAILS_CONTAINER = '#config-details-container',
	CONFIG_HEADING = '#config-heading',
	PROFILES_CONTAINER = '#wconfig-profiles-container',
	PROFILES_ERROR_CONTAINER = '#profiles-error-text',
	SLAS_CONTAINER = '#wconfig-slas-container',
	SLAS_ERROR_CONTAINER = '#slas-error-text',
	MAPPINGS_CONTAINER = '#wconfig-mappings-container',
	MAPPINGS_ERROR_CONTAINER = '#mappings-error-text',
	FEATURE_SELECTOR  = '#wmsconfig-feature-selector',
	PROFILES_SELECTOR = '#wms-profiles-link',
	SLAS_SELECTOR = '#wms-slas-link',
	MAPPINGS_SELECTOR = '#wms-mappings-link',
	PROFILES_BTN = '#profiles-btn',
	SLAS_BTN= '#slas-btn',
	MAPPINGS_BTN = '#mappings-btn',
	REFRESH_ACTION = '#refreshAction';
	
	var WorkloadConfigurationView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			_this = this;
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			_this.processRequest();
		},
		doResume: function(){
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			_this.processRequest();
		},
		doPause: function(){
			workloadProfileConfigurationView.pause();
			workloadSLAConfigurationView.pause();
			workloadMappingConfigurationView.pause();
		},
		selectFeature: function(e){
			$(CONFIG_DETAILS_CONTAINER).show();
			var selectedFeatureLink = PROFILES_SELECTOR;

			if(e && e.target && $(e.target).length > 0){
				selectedFeatureLink = $(e.target)[0].hash;
			}else{
				var ACTIVE_BTN = $(FEATURE_SELECTOR + ' .active');
				var activeButton = null;
				if(ACTIVE_BTN){
					activeButton = '#'+ACTIVE_BTN.attr('id');
				}
				switch(activeButton){
				case PROFILES_BTN:
					selectedFeatureLink = PROFILES_SELECTOR;
					break;
				case SLAS_BTN:
					selectedFeatureLink = SLAS_SELECTOR;
					break;
				case MAPPINGS_BTN:
					selectedFeatureLink = MAPPINGS_SELECTOR;
					break;
				}
			}

			$(PROFILES_CONTAINER).hide();
			$(SLAS_CONTAINER).hide();
			$(MAPPINGS_CONTAINER).hide();
			
			if(workloadProfileConfigurationView != null){
				workloadProfileConfigurationView.pause();
			}
			if(workloadSLAConfigurationView != null){
				workloadSLAConfigurationView.pause();
			}
			if(workloadMappingConfigurationView != null){
				workloadMappingConfigurationView.pause();
			}			
			switch(selectedFeatureLink){
			case PROFILES_SELECTOR:
				$(PROFILES_CONTAINER).show();
				$(CONFIG_HEADING).text("Profiles");
				if(workloadProfileConfigurationView == null){
					workloadProfileConfigurationView = new WorkloadProfileConfigurationView();
					workloadProfileConfigurationView.init();
				}else{
					workloadProfileConfigurationView.resume();
				}
				break;
			case SLAS_SELECTOR:
				$(CONFIG_HEADING).text("SLAs");
				if(workloadSLAConfigurationView == null){
					workloadSLAConfigurationView = new WorkloadSLAConfigurationView();
					workloadSLAConfigurationView.init();
				}else{
					workloadSLAConfigurationView.resume();
				}
				$(SLAS_CONTAINER).show();
				break;
			case MAPPINGS_SELECTOR:
				$(CONFIG_HEADING).text("Mappings");
				if(workloadMappingConfigurationView == null){
					workloadMappingConfigurationView = new WorkloadMappingConfigurationView();
					workloadMappingConfigurationView.init();
				}else{
					workloadMappingConfigurationView.resume();
				}
				$(MAPPINGS_CONTAINER).show();
				break;
			}
		},
		processRequest: function(){
			$(PROFILES_SELECTOR).tab('show');
			_this.selectFeature();
		}		
	});


	return WorkloadConfigurationView;
});
