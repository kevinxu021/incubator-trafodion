//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workload_slas.html',
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

	var slasDataTable = null;
	
	var _this = null;
	var resizeTimer = null;
	var pageStatus = {};
	
	var REFRESH_MENU = '#refreshAction';
	
	var SLAS_SPINNER = '#slas-spinner',
	SLAS_CONTAINER = '#slas-result-container',
	SLAS_ERROR_CONTAINER = '#slas-error-text';
	
	var ADD_SLA_BTN = '#add-sla-btn',
	SLA_DIALOG = '#wsla-dialog',
	SLA_DIALOG_TITLE = "#wsla-dialog-label",
	SLA_DIALOG_SPINNER = '#sla-dialog-spinner',
	ADD_SLA_ERROR_CONTAINER = '#add-sla-error-message',
	SLA_FORM = '#wsla-form',
	SLA_NAME = '#sla_name',
	SLA_PRIORITY = '#sla_priority',
	SLA_LIMIT = '#sla_limit',
	SLA_THROUGHPUT = '#sla_throughput',
	SLA_CONN_PROFILE_NAME = '#conn_profile_name',
	SLA_APPLY_BTN = "#slaApplyButton",
	SLA_RESET_BTN = "#slaResetButton",
	SLA_DELETE_DIALOG = '#sla-delete-dialog',
	DELETE_SLA_NAME = '#delete-sla-name',
	DELETE_SLA_YES_BTN = '#delete-sla-yes-btn';
	
	var slaFormValidator = null;
	var slaDialogParams = null;
	var slaNameColIndex = -1;
	var deleteSLAIconColIndex = 8;
	var dataTableColNames = [];
	
	var WorkloadSLAConfigurationView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			_this = this;
			pageStatus = {};
			$(REFRESH_MENU).on('click', this.doRefresh);
			$(window).on('resize', this.onResize);
			wHandler.on(wHandler.FETCH_PROFILES_SUCCESS, this.displayProfiles);
			wHandler.on(wHandler.FETCH_PROFILES_ERROR, this.fetchProfilesError);
			wHandler.on(wHandler.FETCH_SLAS_SUCCESS, this.displaySLAs);
			wHandler.on(wHandler.FETCH_SLAS_ERROR, this.fetchSLAsError);
			wHandler.on(wHandler.ADDALTER_SLA_SUCCESS, this.addAlterSLASuccess);
			wHandler.on(wHandler.ADDALTER_SLA_ERROR, this.addAlterSLAError);
			wHandler.on(wHandler.DELETE_SLA_SUCCESS, this.deleteSLASuccess);
			wHandler.on(wHandler.DELETE_SLA_ERROR, this.deleteSLAError);
			
			$(ADD_SLA_BTN).on('click', this.addSLABtnClicked);
			$(DELETE_SLA_YES_BTN).on('click', this.deleteSLABtnClicked);
			$(SLA_APPLY_BTN).on('click', this.slaApplyBtnClicked);
			$(SLA_RESET_BTN).on('click', this.slaResetBtnClicked);
			
			$.validator.addMethod("alphanumeric", function(value, element) {
			    return this.optional(element) || /^\w+$/i.test(value);
			}, "Only alphanumeric characters and underscores are allowed");
			
			slaFormValidator = $(SLA_FORM).validate({
				rules: {
					"sla_name": { required: true, alphanumeric: true}
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
			
			$(SLA_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(SLA_APPLY_BTN).attr('disabled', false);
				} else {
					$(SLA_APPLY_BTN).attr('disabled', true);
				}
			});
			
			$(SLA_DIALOG).on('show.bs.modal', function (e) {
				$(SLA_DIALOG_SPINNER).hide();
				_this.fetchProfiles();
				_this.doReset();
			});

			$(SLA_DIALOG).on('hide.bs.modal', function (e, v) {
				$(SLA_NAME).focus();
				_this.doReset();
			});	
			
			$(SLA_DIALOG).on('hidden.bs.modal', function (e, v) {
				slaFormValidator.resetForm();
				$(ADD_SLA_ERROR_CONTAINER).text("");
				$(ADD_SLA_ERROR_CONTAINER).hide();
				$(SLA_DIALOG_TITLE).text('Add Profile');
				$(SLA_NAME).val("");
				_this.fetchSLAs();
			});
			
			_this.fetchSLAs();
		},
		doResume: function(){
			$(REFRESH_MENU).on('click', this.doRefresh);
			$(window).on('resize', this.onResize);
			$(ADD_SLA_BTN).on('click', this.addSLABtnClicked);
			wHandler.on(wHandler.FETCH_PROFILES_SUCCESS, this.displayProfiles);
			wHandler.on(wHandler.FETCH_PROFILES_ERROR, this.fetchProfilesError);
			wHandler.on(wHandler.FETCH_SLAS_SUCCESS, this.displaySLAs);
			wHandler.on(wHandler.FETCH_SLAS_ERROR, this.fetchSLAsError);
			wHandler.on(wHandler.ADDALTER_SLA_SUCCESS, this.addAlterSLASuccess);
			wHandler.on(wHandler.ADDALTER_SLA_ERROR, this.addAlterSLAError);
			wHandler.on(wHandler.DELETE_SLA_SUCCESS, this.deleteSLASuccess);
			wHandler.on(wHandler.DELETE_SLA_ERROR, this.deleteSLAError);
		},
		doPause: function(){
			$(REFRESH_MENU).off('click', this.doRefresh);
			$(window).off('resize', this.onResize);
			wHandler.off(wHandler.FETCH_PROFILES_SUCCESS, this.displayProfiles);
			wHandler.off(wHandler.FETCH_PROFILES_ERROR, this.fetchProfilesError);
			wHandler.off(wHandler.FETCH_SLAS_SUCCESS, this.displaySLAs);
			wHandler.off(wHandler.FETCH_SLAS_ERROR, this.fetchSLAsError);
			wHandler.off(wHandler.ADDALTER_SLA_SUCCESS, this.addAlterSLASuccess);
			wHandler.off(wHandler.ADDALTER_SLA_ERROR, this.addAlterSLAError);
			wHandler.off(wHandler.DELETE_SLA_SUCCESS, this.deleteSLASuccess);
			wHandler.off(wHandler.DELETE_SLA_ERROR, this.deleteSLAError);
			$(ADD_SLA_BTN).off('click', this.addSLABtnClicked);
		},
		
		doRefresh: function(){
			pageStatus.slasFetched = false;
			_this.fetchSLAs();
		},
		fetchProfiles: function () {
			wHandler.fetchProfiles(true);
		},
		fetchSLAs: function () {
			if(!pageStatus.slasFetched || pageStatus.slasFetched == false){
				$(SLAS_SPINNER).show();
				$(SLAS_ERROR_CONTAINER).hide();
				wHandler.fetchSLAs();
			}
		},
		doReset: function() {
			$(ADD_SLA_ERROR_CONTAINER).text("");
			$(ADD_SLA_ERROR_CONTAINER).hide();
			if(slaDialogParams != null){
				if(slaDialogParams.type && slaDialogParams.type == 'add'){
					$(SLA_NAME).attr('disabled', false);
					$(SLA_DIALOG_TITLE).text('Add SLA');
					$(SLA_NAME).val("");
					$(SLA_PRIORITY).val();
					$(SLA_LIMIT).val(slaDialogParams.data["limit"]);
					$(SLA_THROUGHPUT).val(slaDialogParams.data["throughput"]);
					$(SLA_CONN_PROFILE_NAME).val(slaDialogParams.data["onConnectProfile"]);
					//$(CQD_CONTAINER).val(slaDialogParams.data[dataTableColNames.indexOf("cqd")]);
				}
				if(slaDialogParams.type && slaDialogParams.type == 'alter'){
					$(SLA_DIALOG_TITLE).text('Alter SLA');
					$(SLA_NAME).attr('disabled', true);
					$(SLA_NAME).val(slaDialogParams.data["SLA Name"]);
					$(SLA_PRIORITY).val();
					$(SLA_LIMIT).val(slaDialogParams.data["limit"]);
					$(SLA_THROUGHPUT).val(slaDialogParams.data["throughput"]);
					$(SLA_CONN_PROFILE_NAME).val(slaDialogParams.data["onConnectProfile"]);
				}
			}			
		},
		displaySLAs: function (result){
			$(SLAS_SPINNER).hide();
			var keys = result.columnNames;
			$(SLAS_ERROR_CONTAINER).hide();
			pageStatus.slasFetched = true;

			if(keys != null && keys.length > 0) {
				$(SLAS_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="wc-slas-list"></table>';
				$(SLAS_CONTAINER).html( sb );

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
					if(v == 'SLA Name'){
						slaNameColIndex = k;
					}
					if(v == 'lastUpdate'){
						updateTimeColIndex = k;
					}
					aoColumns.push(obj);
					dataTableColNames.push(v);
				});

				var bPaging = aaData.length > 25;

				if(slasDataTable != null) {
					try {
						slasDataTable.clear().draw();
					}catch(Error){

					}
				}
				
				var aoColumnDefs = [];
				if(slaNameColIndex >= 0){
					aoColumnDefs.push({
						"aTargets": [ slaNameColIndex ],
						"mData": slaNameColIndex,
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
								return moment(parseInt(data)).format("YYYY-MM-DD HH:mm:ss");
							}else 
								return data;
						}
					});
				}
				aoColumnDefs.push({
					"aTargets": [ 6 ],
					"mData": 6,
					"visible" : false,
					"searchable" : false
				});
				aoColumnDefs.push({
					"aTargets": [ deleteSLAIconColIndex ],
					"mData": deleteSLAIconColIndex,
					"className": "dt-center",
					"mRender": function ( data, type, full ) {
						if ( type === 'display' ) {
				            return '<a class="fa fa-trash-o"></a>';
				        } else return "";

					}
				});
				slasDataTable = $('#wc-slas-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no slas"
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
					          { extend : 'pdfHtml5', exportOptions: { columns: ':visible', orthogonal: 'export'  }, title: "Workload SLAs", orientation: 'landscape' },
					          { extend : 'print', exportOptions: { columns: ':visible', orthogonal: 'export' }, title: "Workload SLAs" }
					          ],					             
			          fnDrawCallback: function(){
			          }
				});
				
				$('#wc-slas-list tbody').on( 'click', 'td', function (e, a) {
					if(slasDataTable.cell(this)){
						var cell = slasDataTable.cell(this).index();
						if(cell){
							if(cell.column == slaNameColIndex){
								var data = slasDataTable.row(cell.row).data();
								if(data && data.length > 0){
									slaDialogParams = {};
									var sla = {};
									$.each(dataTableColNames, function(j,k){
										sla[k] = data[j];
									});
									slaDialogParams = {type: 'alter', data: sla};
									_this.addSLABtnClicked();
								}
							}else{
								if(cell.column == deleteSLAIconColIndex){
									var data = slasDataTable.row(cell.row).data();
									$(DELETE_SLA_NAME).text(data[slaNameColIndex]);
									$(SLA_DELETE_DIALOG).modal('show');
								}
							}
						}
					}
				})
			}

		},
		fethSLAsError: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				$(SLAS_SPINNER).hide();
				$(SLAS_CONTAINER).hide();
				$(SLAS_ERROR_CONTAINER).show();
				if (jqXHR.responseText) {
					$(SLAS_ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
					if(jqXHR.status != null && jqXHR.status == 0) {
						$(SLAS_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
					}
				}
			}
		},
		onResize: function() {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(_this.doResize, 200);
		},
		doResize: function() {
			if(slasDataTable != null){
				slasDataTable.columns.adjust().draw();
			}
		},
		deleteSLA: function(s){
			wHandler.deleteSLA(s);
		},
		addSLABtnClicked: function(e){
			if(e && e.currentTarget && $(e.currentTarget)[0] == $(ADD_SLA_BTN)[0]){
				slaDialogParams = {};
				if(slasDataTable != null){
					var paramDataRow = [];
					var selectedRows = slasDataTable.rows( { selected: true } );
					if(selectedRows && selectedRows.count() >0){
						paramDataRow = selectedRows.data()[0];
					}else{
						var dataRows = slasDataTable.rows().data();
						$.each(dataRows, function(i, v){
							if(v[dataTableColNames.indexOf("isDefault")] == 'yes'){
								paramDataRow = v;
								return;
							}
						});
					}
					var sla = {};
					$.each(dataTableColNames, function(j,k){
						sla[k] = paramDataRow[j];
					});
					slaDialogParams = {type: 'add', data: sla};
				}
			}
			$(SLA_DIALOG).modal('show');
		},
		slaApplyBtnClicked: function(){
			if(!$(SLA_FORM).valid()) {
				return;
			}
			var sla = {};
			sla.name = $(SLA_NAME).val();
			sla.priority = $(SLA_PRIORITY).val();
			sla.limit = $(SLA_LIMIT).val();
			sla.throughput = $(SLA_THROUGHPUT).val();
			sla.connProfile = $(SLA_CONN_PROFILE_NAME).val();

			$(SLA_DIALOG_SPINNER).show();
			$(SLA_APPLY_BTN).prop("disabled", true);
			$(SLA_RESET_BTN).prop("disabled", true);
			wHandler.addAlterSLA(sla);
		},
		slaResetBtnClicked: function(){
			_this.doReset();
		},
		addAlterSLASuccess: function(data){
			$(ADD_SLA_ERROR_CONTAINER).text("");
			$(ADD_SLA_ERROR_CONTAINER).hide();
			$(SLA_DIALOG_SPINNER).hide();
			$(SLA_APPLY_BTN).prop("disabled", false);
			$(SLA_RESET_BTN).prop("disabled", false);
			pageStatus.slasFetched = false; //enable refetch of data
			$(SLA_DIALOG).modal('hide');
		},
		addAlterSLAError: function(jqXHR){
			$(SLA_DIALOG_SPINNER).hide();
			$(ADD_SLA_ERROR_CONTAINER).show();
			$(SLA_APPLY_BTN).prop("disabled", false);
			$(SLA_RESET_BTN).prop("disabled", false);	
			_this.isAjaxCompleted=true;
			
			var msg = "";
			if (jqXHR.responseText) {
				msg =  "Failed to create SLA : " + jqXHR.responseText;
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					msg = "Error : Unable to communicate with the server.";
				}
			}
			$(ADD_SLA_ERROR_CONTAINER).text(msg);
		},
		deleteSLABtnClicked: function(){
			var slaName = $(DELETE_SLA_NAME).text();
			wHandler.deleteSLA(slaName);
		},
		deleteSLASuccess: function(){
			pageStatus.slasFetched = false;
			_this.fetchSLAs();
		},
		deleteSLAError: function(jqXHR){
			var msg = "";
			if (jqXHR.responseText) {
				msg =  "Failed to delete SLA : " + jqXHR.responseText;
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					msg = "Error : Unable to communicate with the server.";
				}
			}
			alert(msg);
		},
		displayProfiles: function (result){
			$(SLA_CONN_PROFILE_NAME).empty();
			var keys = result.columnNames;
			var profileNameColIndex = -1;
			$.each(keys, function(k, v) {
				if(v == 'Profile Name'){
					profileNameColIndex = k;
				}
			});
			var profileNames = [];
			if(profileNameColIndex >=0){
				$.each(result.resultArray, function(i, data){
					profileNames.push(data[profileNameColIndex]);
				});			
			}
			$.each(profileNames, function(key, value) {   
			     $(SLA_CONN_PROFILE_NAME)
			         .append($("<option></option>")
			                    .attr("value",value)
			                    .text(value)); 
			});
			
			if($.inArray(slaDialogParams.data["onConnectProfile"], profileNames)){
				$(SLA_CONN_PROFILE_NAME).val(slaDialogParams.data["onConnectProfile"]);
			}
		},
		fetchProfilesError: function (jqXHR) {
			
		}		
	});


	return WorkloadSLAConfigurationView;
});
