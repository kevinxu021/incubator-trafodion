//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workload_profiles.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datatables.net-buttons',
        'datatables.net-select',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common) {
	'use strict';

	var profilesDataTable = null;
	
	var _this = null;
	var resizeTimer = null;
	var pageStatus = {};
	
	var REFRESH_MENU = '#refreshAction';
	
	var PROFILES_SPINNER = '#profiles-spinner',
	PROFILES_CONTAINER = '#profiles-result-container',
	PROFILES_ERROR_CONTAINER = '#profiles-error-text';
	
	var ADD_PROFILE_BTN = '#add-profile-btn',
	PROFILE_DIALOG = '#wprofile-dialog',
	PROFILE_DIALOG_TITLE = "#wprofile-dialog-label",
	PROFILE_DIALOG_SPINNER = '#profile-dialog-spinner',
	ADD_PROFILE_ERROR_CONTAINER = '#add-profile-error-message',
	PROFILE_FORM = '#wprofile-form',
	PROFILE_NAME = '#profile_name',
	CQD_CONTAINER = '#profile-cqd-stmts',
	SET_CONTAINER = '#profile-set-stmts',
	PROFILE_NODES = '#profile-host-list',
	PROFILE_APPLY_BTN = "#profileApplyButton",
	PROFILE_RESET_BTN = "#profileResetButton",
	PROFILE_DELETE_DIALOG = '#profile-delete-dialog',
	DELETE_PROFILE_NAME = '#delete-profile-name',
	DELETE_PROFILE_YES_BTN = '#delete-profile-yes-btn';
	
	var profileFormValidator = null;
	var profileDialogParams = null;
	var profileNameColIndex = -1;
	var deleteProfileIconColIndex = 6;
	var dataTableColNames = [];
	
	var WorkloadProfileConfigurationView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			_this = this;
			pageStatus = {};
			$(REFRESH_MENU).on('click', this.doRefresh);
			$(window).on('resize', this.onResize);
			wHandler.on(wHandler.FETCH_PROFILES_SUCCESS, this.displayProfiles);
			wHandler.on(wHandler.FETCH_PROFILES_ERROR, this.fetchProfilesError);
			wHandler.on(wHandler.ADDALTER_PROFILE_SUCCESS, this.addAlterProfileSuccess);
			wHandler.on(wHandler.ADDALTER_PROFILE_ERROR, this.addAlterProfileError);
			wHandler.on(wHandler.DELETE_PROFILE_SUCCESS, this.deleteProfileSuccess);
			wHandler.on(wHandler.DELETE_PROFILE_ERROR, this.deleteProfileError);
			
			$(ADD_PROFILE_BTN).on('click', this.addProfileBtnClicked);
			$(DELETE_PROFILE_YES_BTN).on('click', this.deleteProfileBtnClicked);
			$(PROFILE_APPLY_BTN).on('click', this.profileApplyBtnClicked);
			$(PROFILE_RESET_BTN).on('click', this.profileResetBtnClicked);
			
			$.validator.addMethod("alphanumeric", function(value, element) {
			    return this.optional(element) || /^\w+$/i.test(value);
			}, "Only alphanumeric characters and underscores are allowed");
			
			$.validator.addMethod("cqdssets", function(value, element) {
			    return this.optional(element) || /^[\w\-;_ '"]*$/i.test(value);
			}, "Only alphanumeric characters, underscores, semicolons, spaces and single/double quotes are allowed");
			
			profileFormValidator = $(PROFILE_FORM).validate({
				rules: {
					"profile_name": { required: true, alphanumeric: true},
					"profile-cqd-stmts": { cqdssets: true},
					"profile-set-stmts": { cqdssets: true}
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
			
			$(PROFILE_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(PROFILE_APPLY_BTN).attr('disabled', false);
				} else {
					$(PROFILE_APPLY_BTN).attr('disabled', true);
				}
			});
			
			$(PROFILE_DIALOG).on('show.bs.modal', function (e) {
				$(PROFILE_DIALOG_SPINNER).hide();

			});
			
			$(PROFILE_DIALOG).on('shown.bs.modal', function (e) {
				$(PROFILE_NAME).focus();
				_this.doReset();
			});


			$(PROFILE_DIALOG).on('hide.bs.modal', function (e, v) {
				profileFormValidator.resetForm();
				$(ADD_PROFILE_ERROR_CONTAINER).text("");
				$(ADD_PROFILE_ERROR_CONTAINER).hide();
				$(PROFILE_DIALOG_TITLE).text('Add Profile');
				$(PROFILE_NAME).val("");
				$(CQD_CONTAINER).val("");
				$(SET_CONTAINER).val("");
				$(PROFILE_NODES).val("");
				_this.fetchProfiles();
			});	
			
			_this.fetchProfiles();
		},
		doResume: function(){
			$(REFRESH_MENU).on('click', this.doRefresh);
			$(window).on('resize', this.onResize);
			$(ADD_PROFILE_BTN).on('click', this.addProfileBtnClicked);
			wHandler.on(wHandler.FETCH_PROFILES_SUCCESS, this.displayProfiles);
			wHandler.on(wHandler.FETCH_PROFILES_ERROR, this.fetchProfilesError);
			wHandler.on(wHandler.ADDALTER_PROFILE_SUCCESS, this.addAlterProfileSuccess);
			wHandler.on(wHandler.ADDALTER_PROFILE_ERROR, this.addAlterProfileError);
			wHandler.on(wHandler.DELETE_PROFILE_SUCCESS, this.deleteProfileSuccess);
			wHandler.on(wHandler.DELETE_PROFILE_ERROR, this.deleteProfileError);
		},
		doPause: function(){
			$(REFRESH_MENU).off('click', this.doRefresh);
			$(window).off('resize', this.onResize);
			wHandler.off(wHandler.FETCH_PROFILES_SUCCESS, this.displayProfiles);
			wHandler.off(wHandler.FETCH_PROFILES_ERROR, this.fetchProfilesError);
			wHandler.off(wHandler.ADDALTER_PROFILE_SUCCESS, this.addAlterProfileSuccess);
			wHandler.off(wHandler.ADDALTER_PROFILE_ERROR, this.addAlterProfileError);
			wHandler.off(wHandler.DELETE_PROFILE_SUCCESS, this.deleteProfileSuccess);
			wHandler.off(wHandler.DELETE_PROFILE_ERROR, this.deleteProfileError);
			$(ADD_PROFILE_BTN).off('click', this.addProfileBtnClicked);
		},
		doRefresh: function(){
			pageStatus.profilesFetched = false;
			_this.fetchProfiles();
		},
		fetchProfiles: function () {
			if(!pageStatus.profilesFetched || pageStatus.profilesFetched == false){
				$(PROFILES_SPINNER).show();
				$(PROFILES_ERROR_CONTAINER).hide();
				wHandler.fetchProfiles();
			}
		},
		doReset: function() {
			$(ADD_PROFILE_ERROR_CONTAINER).text("");
			$(ADD_PROFILE_ERROR_CONTAINER).hide();
			if(profileDialogParams != null){
				if(profileDialogParams.type && profileDialogParams.type == 'add'){
					$(PROFILE_NAME).attr('disabled', false);
					$(PROFILE_DIALOG_TITLE).text('Add Profile');
					$(PROFILE_NAME).val("");
					$(CQD_CONTAINER).val(profileDialogParams.data["cqd"]);
					$(SET_CONTAINER).val(profileDialogParams.data["set"]);
					$(PROFILE_NODES).val(profileDialogParams.data["hostList"]);
				}
				if(profileDialogParams.type && profileDialogParams.type == 'alter'){
					$(PROFILE_DIALOG_TITLE).text('Alter Profile');
					$(PROFILE_NAME).attr('disabled', true);
					$(PROFILE_NAME).val(profileDialogParams.data["Profile Name"]);
					$(CQD_CONTAINER).val(profileDialogParams.data["cqd"]);
					$(SET_CONTAINER).val(profileDialogParams.data["set"]);
					$(PROFILE_NODES).val(profileDialogParams.data["hostList"]);
				}
			}			
		},
		displayProfiles: function (result){
			$(PROFILES_SPINNER).hide();
			var keys = result.columnNames;
			$(PROFILES_ERROR_CONTAINER).hide();
			pageStatus.profilesFetched = true;

			if(keys != null && keys.length > 0) {
				$(PROFILES_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="wc-profiles-list"></table>';
				$(PROFILES_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];

				$.each(result.resultArray, function(i, data){
					data.push(null); //for edit/delete link
					aaData.push(data);
				});
				
				dataTableColNames = [];
				var updateTimeColIndex = -1;
				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = common.UpperCaseFirst(v);
					if(v == 'Profile Name'){
						profileNameColIndex = k;
					}
					if(v == 'lastUpdate'){
						updateTimeColIndex = k;
					}
					aoColumns.push(obj);
					dataTableColNames.push(v);
				});
				
				var bPaging = aaData.length > 25;

				if(profilesDataTable != null) {
					try {
						profilesDataTable.clear().draw();
					}catch(Error){

					}
				}
				
				var aoColumnDefs = [];
				if(profileNameColIndex >= 0){
					aoColumnDefs.push({
						"aTargets": [ profileNameColIndex ],
						"mData": profileNameColIndex,
						"mRender": function ( data, type, full ) {
							if(type == 'display') {
								var rowcontent = '<a style="cursor:pointer"> ' + data + '</a>';
								return rowcontent;                         
							}else { 
								return data;
							}
						}
					});
				}
				if(updateTimeColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ updateTimeColIndex ],
						"mData": updateTimeColIndex,
						"mRender": function ( data, type, full ) {
							if(type == 'display'){
								return common.toServerLocalDateFromMilliSeconds(parseInt(data), 'YYYY-MM-DD HH:mm:ss');
							}else 
								return data;
						}
					});
				}	
				aoColumnDefs.push({
					"aTargets": [ deleteProfileIconColIndex ],
					"mData": deleteProfileIconColIndex,
					"className": "dt-center",
					"mRender": function ( data, type, full ) {
						if ( type === 'display' ) {
				            return '<a class="delete-profile fa fa-trash-o"></a>';
				        } else return "";

					}
				});
				
				profilesDataTable = $('#wc-profiles-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no profiles"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					autoWidth: true,
					select: {style: 'single', items: 'row', info: false},
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs" : aoColumnDefs,
					"order": [[ 0, "asc" ]],
					buttons: [
					          { extend : 'copy', exportOptions: { columns: ':visible', orthogonal: 'export'  } },
					          { extend : 'csv', exportOptions: { columns: ':visible', orthogonal: 'export' } },
					          //{ extend : 'excel', exportOptions: { columns: ':visible', orthogonal: 'export' } },
					          { extend : 'pdfHtml5', exportOptions: { columns: ':visible', orthogonal: 'export'  }, title: "Workload Profiles", orientation: 'landscape' },
					          { extend : 'print', exportOptions: { columns: ':visible', orthogonal: 'export' }, title: "Workload Profiles" }
					          ],					             
			          fnDrawCallback: function(){
			          }
				});
				
				$('#wc-profiles-list tbody').on( 'click', 'td', function (e, a) {
					if(profilesDataTable.cell(this)){
						var cell = profilesDataTable.cell(this).index();
						if(cell){
							if(cell.column == profileNameColIndex){
								var data = profilesDataTable.row(cell.row).data();
								if(data && data.length > 0){
									profileDialogParams = {};
									var profile = {};
									$.each(dataTableColNames, function(j,k){
										profile[k] = data[j];
									});
									profileDialogParams = {type: 'alter', data: profile};
									_this.addProfileBtnClicked();
								}
							}else{
								if(cell.column == deleteProfileIconColIndex){
									var data = profilesDataTable.row(cell.row).data();
									$(DELETE_PROFILE_NAME).text(data[profileNameColIndex]);
									$(PROFILE_DELETE_DIALOG).modal('show');
								}
							}
						}
					}
				})
			}

		},
		fetchProfilesError: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				$(PROFILES_SPINNER).hide();
				$(PROFILES_CONTAINER).hide();
				$(PROFILES_ERROR_CONTAINER).show();
				if (jqXHR.responseText) {
					$(PROFILES_ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
					if(jqXHR.status != null && jqXHR.status == 0) {
						$(PROFILES_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
					}
				}
			}
		},
		onResize: function() {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(_this.doResize, 200);
		},
		doResize: function() {
			if(profilesDataTable != null){
				profilesDataTable.columns.adjust().draw();
			}
		},
		deleteProfile: function(p){
			wHandler.deleteProfile(p);
		},
		addProfileBtnClicked: function(e){
			if(e && e.currentTarget && $(e.currentTarget)[0] == $(ADD_PROFILE_BTN)[0]){
				profileDialogParams = {};
				if(profilesDataTable != null){
					var paramDataRow = [];
					var selectedRows = profilesDataTable.rows( { selected: true } );
					if(selectedRows && selectedRows.count() >0){
						paramDataRow = selectedRows.data()[0];
					}else{
						var dataRows = profilesDataTable.rows().data();
						$.each(dataRows, function(i, v){
							if(v[dataTableColNames.indexOf("isDefault")] == 'yes'){
								paramDataRow = v;
								return;
							}
						});
					}
					var p = {};
					$.each(dataTableColNames, function(j,k){
						p[k] = paramDataRow[j];
					});
					profileDialogParams = {type: 'add', data: p};
				}
			}
			$(PROFILE_DIALOG).modal('show');
		},
		profileApplyBtnClicked: function(){
			if(!$(PROFILE_FORM).valid()) {
				return;
			}
			var profile = {};
			profile.name = $(PROFILE_NAME).val();
			profile.cqds = $(CQD_CONTAINER).val();
			profile.sets = $(SET_CONTAINER).val();
			profile.nodes = $(PROFILE_NODES).val();

			$(PROFILE_DIALOG_SPINNER).show();
			$(PROFILE_APPLY_BTN).prop("disabled", true);
			$(PROFILE_RESET_BTN).prop("disabled", true);
			wHandler.addAlterProfile(profile);
		},
		profileResetBtnClicked: function(){
			_this.doReset();
		},
		addAlterProfileSuccess: function(data){
			$(ADD_PROFILE_ERROR_CONTAINER).text("");
			$(ADD_PROFILE_ERROR_CONTAINER).hide();
			$(PROFILE_DIALOG_SPINNER).hide();
			$(PROFILE_APPLY_BTN).prop("disabled", false);
			$(PROFILE_RESET_BTN).prop("disabled", false);
			pageStatus.profilesFetched = false; //enable refetch of data
			$(PROFILE_DIALOG).modal('hide');
		},
		addAlterProfileError: function(jqXHR){
			$(PROFILE_DIALOG_SPINNER).hide();
			$(ADD_PROFILE_ERROR_CONTAINER).show();
			$(PROFILE_APPLY_BTN).prop("disabled", false);
			$(PROFILE_RESET_BTN).prop("disabled", false);	
			_this.isAjaxCompleted=true;
			
			var msg = "";
			if (jqXHR.responseText) {
				msg =  "Failed to create profile : " + jqXHR.responseText;
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					msg = "Error : Unable to communicate with the server.";
				}
			}
			$(ADD_PROFILE_ERROR_CONTAINER).text(msg);
		},
		deleteProfileBtnClicked: function(){
			var profileName = $(DELETE_PROFILE_NAME).text();
			wHandler.deleteProfile(profileName);
		},
		deleteProfileSuccess: function(){
			pageStatus.profilesFetched = false;
			_this.fetchProfiles();
		},
		deleteProfileError: function(jqXHR){
			var msg = "";
			if (jqXHR.responseText) {
				msg =  "Failed to delete profile : " + jqXHR.responseText;
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					msg = "Error : Unable to communicate with the server.";
				}
			}
			alert(msg);		
		}
	});


	return WorkloadProfileConfigurationView;
});
