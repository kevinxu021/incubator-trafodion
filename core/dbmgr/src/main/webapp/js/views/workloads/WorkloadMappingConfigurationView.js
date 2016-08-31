//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workload_mappings.html',
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

	var mappingsDataTable = null;

	var _this = null;
	var resizeTimer = null;
	var pageStatus = {};

	var REFRESH_MENU = '#refreshAction';

	var MAPPINGS_SPINNER = '#mappings-spinner',
	MAPPINGS_CONTAINER = '#mappings-result-container',
	MAPPINGS_ERROR_CONTAINER = '#mappings-error-text';

	var ADD_MAPPING_BTN = '#add-mapping-btn',
	MAPPING_DIALOG = '#mapping-dialog',
	MAPPING_DIALOG_TITLE = "#mapping-dialog-label",
	MAPPING_DIALOG_SPINNER = '#mapping-dialog-spinner',
	MAPPING_FORM = '#mapping-form',
	MAPPING_NAME = '#mapping_name',
	MAPPING_USER = '#mapping_user',
	MAPPING_APPLICATION = '#mapping_application',
	MAPPING_ROLE = '#mapping_role',
	MAPPING_SESSION = '#mapping_session',
	MAPPING_CLIENT_IP = '#mapping_client_ip',
	MAPPING_CLIENT_HOST = '#mapping_client_host',
	MAPPING_SLA = '#mapping_sla',
	MAPPING_STATE = '#mapping_state',
	MAPPING_SEQ_NO = '#mapping_seq_no',
	MAPPING_APPLY_BTN = "#mappingApplyButton",
	MAPPING_RESET_BTN = "#mappingResetButton",
	MAPPING_DELETE_DIALOG = '#mapping-delete-dialog',
	DELETE_MAPPING_NAME = '#delete-mapping-name',
	DELETE_MAPPING_YES_BTN = '#delete-mapping-yes-btn',
	ADD_MAPPING_ERROR_CONTAINER = '#add-mapping-error-message';

	var mappingFormValidator = null;
	var mappingDialogParams = null;
	var mappingNameColIndex = -1;
	var deleteMappingIconColIndex = 12;
	var dataTableColNames = [];

	var WorkloadMappingConfigurationView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			_this = this;
			pageStatus = {};
			$(REFRESH_MENU).on('click', this.doRefresh);
			$(window).on('resize', this.onResize);
			wHandler.on(wHandler.FETCH_MAPPINGS_SUCCESS, this.displayMappings);
			wHandler.on(wHandler.FETCH_MAPPINGS_ERROR, this.fetchMappingsError);
			wHandler.on(wHandler.ADDALTER_MAPPING_SUCCESS, this.addAlterMappingSuccess);
			wHandler.on(wHandler.ADDALTER_MAPPING_ERROR, this.addAlterMappingError);
			wHandler.on(wHandler.DELETE_MAPPING_SUCCESS, this.deleteMappingSuccess);
			wHandler.on(wHandler.DELETE_MAPPING_ERROR, this.deleteMappingError);
			wHandler.on(wHandler.FETCH_SLAS_SUCCESS, this.displaySLAs);
			wHandler.on(wHandler.FETCH_SLAS_ERROR, this.fetchSLAsError);

			$(ADD_MAPPING_BTN).on('click', this.addMappingBtnClicked);
			$(DELETE_MAPPING_YES_BTN).on('click', this.deleteMappingBtnClicked);
			$(MAPPING_APPLY_BTN).on('click', this.mappingApplyBtnClicked);
			$(MAPPING_RESET_BTN).on('click', this.mappingResetBtnClicked);

			$.validator.addMethod("wmsmap_alphanumeric", function(value, element) {
				if(mappingDialogParams.type && mappingDialogParams.type == 'alter')
					return true; // For alter we don't allow editing the name,so no check needed
				
				return this.optional(element) || /^\w+$/i.test(value);
			}, "Only alphanumeric characters and underscores are allowed");
			
			$.validator.addMethod("wms_ordernumber", function(value, element) {
				return value > 0 && value <= 98;
			}, "Order number has to between 1 and 98");

			mappingFormValidator = $(MAPPING_FORM).validate({
				rules: {
					"mapping_name": { required: true, wmsmap_alphanumeric: true},
					"mapping_seq_no": { required: true, digits: true, wms_ordernumber: true},
					"mapping_state": { required: true},
					"mapping_sla": { required: true}
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

			$(MAPPING_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(MAPPING_APPLY_BTN).attr('disabled', false);
				} else {
					$(MAPPING_APPLY_BTN).attr('disabled', true);
				}
			});

			$(MAPPING_DIALOG).on('show.bs.modal', function (e) {
				$(MAPPING_DIALOG_SPINNER).hide();
				_this.fetchSLAs();
			});

			$(MAPPING_DIALOG).on('shown.bs.modal', function (e) {
				$(MAPPING_NAME).focus();
				_this.doReset();
			});

			$(MAPPING_DIALOG).on('hide.bs.modal', function (e, v) {
				mappingFormValidator.resetForm();
				$(MAPPING_NAME).focus();
				_this.doReset();
			});	

			$(MAPPING_DIALOG).on('hidden.bs.modal', function (e, v) {
				$(ADD_MAPPING_ERROR_CONTAINER).text("");
				$(ADD_MAPPING_ERROR_CONTAINER).hide();
				$(MAPPING_DIALOG_TITLE).text('Add Mapping');
				$(MAPPING_NAME).val("");
				_this.fetchMappings();
			});
			
			$('input').on('blur', function() {
			    if ($(MAPPING_FORM).valid()) {
			        $(MAPPING_APPLY_BTN).prop('disabled', false);  
			    } else {
			        $(MAPPING_APPLY_BTN).prop('disabled', 'disabled');
			    }
			});

			_this.fetchMappings();
		},
		doResume: function(){
			$(REFRESH_MENU).on('click', this.doRefresh);
			$(window).on('resize', this.onResize);
			$(ADD_MAPPING_BTN).on('click', this.addMappingBtnClicked);
			$(DELETE_MAPPING_YES_BTN).on('click', this.deleteMappingBtnClicked);
			wHandler.on(wHandler.FETCH_MAPPINGS_SUCCESS, this.displayMappings);
			wHandler.on(wHandler.FETCH_MAPPINGS_ERROR, this.fetchMappingsError);
			wHandler.on(wHandler.ADDALTER_MAPPING_SUCCESS, this.addAlterMappingSuccess);
			wHandler.on(wHandler.ADDALTER_MAPPING_ERROR, this.addAlterMappingError);
			wHandler.on(wHandler.DELETE_MAPPING_SUCCESS, this.deleteMappingSuccess);
			wHandler.on(wHandler.DELETE_MAPPING_ERROR, this.deleteMappingError);
			wHandler.on(wHandler.FETCH_SLAS_SUCCESS, this.displaySLAs);
			wHandler.on(wHandler.FETCH_SLAS_ERROR, this.fetchSLAsError);
		},
		doPause: function(){
			$(REFRESH_MENU).off('click', this.doRefresh);
			$(window).off('resize', this.onResize);
			wHandler.off(wHandler.FETCH_MAPPINGS_SUCCESS, this.displayMappings);
			wHandler.off(wHandler.FETCH_MAPPINGS_ERROR, this.fetchMappingsError);
			wHandler.off(wHandler.ADDALTER_MAPPING_SUCCESS, this.addAlterMappingSuccess);
			wHandler.off(wHandler.ADDALTER_MAPPING_ERROR, this.addAlterMappingError);
			wHandler.off(wHandler.DELETE_MAPPING_SUCCESS, this.deleteMappingSuccess);
			wHandler.off(wHandler.DELETE_MAPPING_ERROR, this.deleteMappingError);
			wHandler.off(wHandler.FETCH_SLAS_SUCCESS, this.displaySLAs);
			wHandler.off(wHandler.FETCH_SLAS_ERROR, this.fetchSLAsError);
			$(ADD_MAPPING_BTN).off('click', this.addMappingBtnClicked);
			$(DELETE_MAPPING_YES_BTN).on('click', this.deleteMappingBtnClicked);
		},
		doRefresh: function(){
			pageStatus.mappingsFetched = false;
			_this.fetchMappings();
		},
		fetchSLAs: function(){
			wHandler.fetchSLAs(true);
		},
		fetchMappings: function () {
			if(!pageStatus.mappingsFetched || pageStatus.mappingsFetched == false){
				$(MAPPINGS_SPINNER).show();
				$(MAPPINGS_ERROR_CONTAINER).hide();
				wHandler.fetchMappings();
			}
		},
		doReset: function(){
			$(ADD_MAPPING_ERROR_CONTAINER).text("");
			$(ADD_MAPPING_ERROR_CONTAINER).hide();
			if(mappingDialogParams != null){
				if(mappingDialogParams.type && mappingDialogParams.type == 'add'){
					$(MAPPING_NAME).attr('disabled', false);
					$('#mapping-form input, select').prop('disabled', false);
					$(MAPPING_APPLY_BTN).attr('disabled', false);
					$(MAPPING_RESET_BTN).attr('disabled', false);
					
					$(MAPPING_DIALOG_TITLE).text('Add Mapping');
					$(MAPPING_NAME).val("");
					var state = mappingDialogParams.data["isActive"];
					$(MAPPING_STATE).val(state != null ? state.toLowerCase() : "");

					$(MAPPING_USER).val(mappingDialogParams.data["userName"]);
					$(MAPPING_APPLICATION).val(mappingDialogParams.data["applicationName"]);
					$(MAPPING_ROLE).val(mappingDialogParams.data["roleName"]);
					$(MAPPING_SESSION).val(mappingDialogParams.data["sessionName"]);
					$(MAPPING_CLIENT_IP).val(mappingDialogParams.data["clientIpAddress"]);
					$(MAPPING_CLIENT_HOST).val(mappingDialogParams.data["clientHostName"]);
					$(MAPPING_SLA).val(mappingDialogParams.data["sla"]);
					$(MAPPING_SEQ_NO).val("");					
				}
				if(mappingDialogParams.type && mappingDialogParams.type == 'alter'){
					if(mappingDialogParams.data["isDefault"] == 'yes'){
						$('#mapping-form input, select').prop('disabled', true);
						$(MAPPING_APPLY_BTN).attr('disabled', true);
						$(MAPPING_RESET_BTN).attr('disabled', true);
					}else{
						$('#mapping-form input, select').prop('disabled', false);
						$(MAPPING_APPLY_BTN).attr('disabled', false);
						$(MAPPING_RESET_BTN).attr('disabled', false);
					}					
					$(MAPPING_DIALOG_TITLE).text('Alter Mapping');
					$(MAPPING_NAME).attr('disabled', true);
					$(MAPPING_NAME).val(mappingDialogParams.data["name"]);
					var state = mappingDialogParams.data["isActive"];
					$(MAPPING_STATE).val(state != null ? state.toLowerCase() : "");

					$(MAPPING_USER).val(mappingDialogParams.data["userName"]);
					$(MAPPING_APPLICATION).val(mappingDialogParams.data["applicationName"]);
					$(MAPPING_ROLE).val(mappingDialogParams.data["roleName"]);
					$(MAPPING_SESSION).val(mappingDialogParams.data["sessionName"]);
					$(MAPPING_CLIENT_IP).val(mappingDialogParams.data["clientIpAddress"]);
					$(MAPPING_CLIENT_HOST).val(mappingDialogParams.data["clientHostName"]);
					$(MAPPING_SLA).val(mappingDialogParams.data["sla"]);
					$(MAPPING_SEQ_NO).val(mappingDialogParams.data["orderNumber"]);					
				}
			}				
		},
		displayMappings: function (result){
			$(MAPPINGS_SPINNER).hide();
			var keys = result.columnNames;
			$(MAPPINGS_ERROR_CONTAINER).hide();
			pageStatus.mappingsFetched = true;

			if(keys != null && keys.length > 0) {
				$(MAPPINGS_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="wc-mappings-list"></table>';
				$(MAPPINGS_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];

				$.each(result.resultArray, function(i, data){
					data.push(null); //for edit/delete link
					aaData.push(data);
				});

				dataTableColNames = [];
				var updateTimeColIndex = -1;
				var isDefColIndex = -1;
				var stateColIndex = -1;
				var orderNumColIndex = -1;
				var slaColIndex = -1;
				
				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = common.UpperCaseFirst(v);
					if(v == 'name'){
						mappingNameColIndex = k;
					}
					if(v == 'lastUpdate'){
						updateTimeColIndex = k;
						obj.title = 'Last Update Time';
					}
					if(v == 'isDefault'){
						isDefColIndex = k;
					}
					if(v == 'isActive'){
						stateColIndex = k;
					}
					if(v == 'orderNumber'){
						orderNumColIndex = k;
					}
					if(v == 'sla'){
						slaColIndex = k;
					}
					aoColumns.push(obj);
					dataTableColNames.push(v);
				});

				var bPaging = aaData.length > 25;

				if(mappingsDataTable != null) {
					try {
						mappingsDataTable.clear().draw();
					}catch(Error){

					}
				}

				var aoColumnDefs = [];
				if(mappingNameColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ mappingNameColIndex ],
						"mData": mappingNameColIndex,
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
				if(slaColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ slaColIndex ],
						"mData": slaColIndex,
						"mRender": function ( data, type, full ) {
							if(data != null && data != 'null'){
								return data;
							}else 
								return "";
						}
					});
				}
				if(orderNumColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ orderNumColIndex ],
						"mData": orderNumColIndex,
						"className" : "dt-body-right"
					});
					
				}
				if(updateTimeColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ updateTimeColIndex ],
						"mData": updateTimeColIndex,
						"className" : "dt-body-right",
						"mRender": function ( data, type, full ) {
							if(data != null){
								return common.toServerLocalDateFromMilliSeconds(parseInt(data), 'YYYY-MM-DD HH:mm:ss');
							}else 
								return data;
						}
					});
				}	
				aoColumnDefs.push({
					"aTargets": [ deleteMappingIconColIndex ],
					"mData": deleteMappingIconColIndex,
					"className": "dt-center",
					"mRender": function ( data, type, full, meta ) {
						if(type === 'display'){
							var aoColumns = meta.settings.aoColumns;
							var defColIndex = -1;
							
							$.each(aoColumns, function(i, v){
								if(v.title == 'IsDefault'){
									defColIndex = i;
									return;
								}
							});
							
							if(defColIndex >= 0 && full[defColIndex] == 'no'){
								return '<a class="delete-profile fa fa-trash-o"></a>';
							}
							else return "";
							
						} else return "";
					}
				});
				
				if(isDefColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [isDefColIndex ],
						"mData": isDefColIndex,
						"visible" : false,
						"searchable" : false
					});					
				}
				
				if(stateColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ stateColIndex ],
						"mData": stateColIndex,
						"mRender": function ( data, type, full ) {
							return data != null && data != 'null' ? common.UpperCaseFirst(data) : '';
						}
					});
				}
				
				mappingsDataTable = $('#wc-mappings-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no mappings"
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
					"order": [[ 8, "asc" ]],
					buttons: [
					          { extend : 'copy', exportOptions: { columns: ':visible', orthogonal: 'export'  } },
					          { extend : 'csv', exportOptions: { columns: ':visible', orthogonal: 'export' }, filename: 'Workload Mappings' },
					          //{ extend : 'excel', exportOptions: { columns: ':visible', orthogonal: 'export' } },
					          { extend : 'pdfHtml5', exportOptions: { columns: ':visible', orthogonal: 'export'  }, title: "Workload Mappings", orientation: 'landscape' },
					          { extend : 'print', exportOptions: { columns: ':visible', orthogonal: 'export' }, title: "Workload Mappings" }
					          ],					             
					          fnDrawCallback: function(){
					          }
				});

				$('#wc-mappings-list tbody').on( 'click', 'td', function (e, a) {
					if(mappingsDataTable.cell(this)){
						var cell = mappingsDataTable.cell(this).index();
						if(cell){
							if(cell.column == mappingNameColIndex){
								var data = mappingsDataTable.row(cell.row).data();
								if(data && data.length > 0){
									mappingDialogParams = {};
									var map = {};
									$.each(dataTableColNames, function(j,k){
										map[k] = data[j];
									});
									mappingDialogParams = {type: 'alter', data: map};	
									_this.addMappingBtnClicked();
								}
							}else{
								if(cell.column == deleteMappingIconColIndex){
									var data = mappingsDataTable.row(cell.row).data();
									if(data[isDefColIndex] == 'no'){
										$(DELETE_MAPPING_NAME).text(data[mappingNameColIndex]);
										$(MAPPING_DELETE_DIALOG).modal('show');
									}
								}
							}
						}
					}
				})
			}

		},
		fethMappingsError: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				$(MAPPINGS_SPINNER).hide();
				$(MAPPINGS_RESULT_CONTAINER).hide();
				$(MAPPINGS_ERROR_CONTAINER).show();
				if (jqXHR.responseText) {
					$(MAPPINGS_ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
					if(jqXHR.status != null && jqXHR.status == 0) {
						$(MAPPINGS_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
					}
				}
			}
		},
		onResize: function() {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(_this.doResize, 200);
		},
		doResize: function() {
			if(mappingsDataTable != null){
				mappingsDataTable.columns.adjust().draw();
			}
		},
		deleteMapping: function(m){
			wHandler.deleteMapping(m);
		},
		addMappingBtnClicked: function(e){
			if(e && e.currentTarget && $(e.currentTarget)[0] == $(ADD_MAPPING_BTN)[0]){
				mappingDialogParams = {};
				if(mappingsDataTable != null){
					var paramDataRow = [];
					var selectedRows = mappingsDataTable.rows( { selected: true } );
					if(selectedRows && selectedRows.count() >0){
						paramDataRow = selectedRows.data()[0];
					}else{
						var dataRows = mappingsDataTable.rows().data();
						$.each(dataRows, function(i, v){
							if(v[dataTableColNames.indexOf("isDefault")] == 'yes'){
								paramDataRow = v;
								return;
							}
						});
					}
					var map = {};
					$.each(dataTableColNames, function(j,k){
						map[k] = paramDataRow[j];
					});
					mappingDialogParams = {type: 'add', data: map};					
				}
			}
			$(MAPPING_DIALOG).modal('show');
		},
		mappingApplyBtnClicked: function(){
			if(!$(MAPPING_FORM).valid()) {
				return;
			}
			var mapping = {};
			mapping.action = mappingDialogParams.type;
			mapping.name = $(MAPPING_NAME).val();
			mapping.user = $(MAPPING_USER).val();
			mapping.application = $(MAPPING_APPLICATION).val();
			mapping.role = $(MAPPING_ROLE).val();
			mapping.session = $(MAPPING_SESSION).val();
			mapping.clientIP = $(MAPPING_CLIENT_IP).val();
			mapping.clientHost = $(MAPPING_CLIENT_HOST).val();
			mapping.sla = $(MAPPING_SLA).val();
			mapping.seqNo = $(MAPPING_SEQ_NO).val();
			mapping.isActive = $(MAPPING_STATE).val();
			
			if(mapping.application.length == 0 &&
					mapping.user.length == 0 && 
					mapping.role.length == 0 && 
					mapping.session.length == 0 && 
					mapping.clientIP.length == 0 && 
					mapping.clientHost.length == 0 
			) {
				var msgPrefix =  "Failed to create mapping ";
				var msg= "Error : You need to define at least one mapping criteria"
					var msgObj={msg:msg,tag:"danger",url:_this.currentURL,shortMsg:msgPrefix};
				//_this.popupNotificationMessage(null,msgObj);
				alert(msg);
				return;
			}

			$(MAPPING_DIALOG_SPINNER).show();
			$(MAPPING_APPLY_BTN).prop("disabled", true);
			$(MAPPING_RESET_BTN).prop("disabled", true);
			wHandler.addAlterMapping(mapping);
		},
		mappingResetBtnClicked: function(){
			_this.doReset();
			mappingFormValidator.resetForm();
		},
		addAlterMappingSuccess: function(data){
			$(ADD_MAPPING_ERROR_CONTAINER).text("");
			$(ADD_MAPPING_ERROR_CONTAINER).hide();
			$(MAPPING_DIALOG_SPINNER).hide();
			$(MAPPING_APPLY_BTN).prop("disabled", false);
			$(MAPPING_RESET_BTN).prop("disabled", false);
			pageStatus.mappingsFetched = false; //enable refetch of data
			$(MAPPING_DIALOG).modal('hide');
		},
		addAlterMappingError: function(jqXHR){
			$(MAPPING_DIALOG_SPINNER).hide();
			$(ADD_MAPPING_ERROR_CONTAINER).show();
			$(MAPPING_APPLY_BTN).prop("disabled", false);
			$(MAPPING_RESET_BTN).prop("disabled", false);	
			_this.isAjaxCompleted=true;

			var msg = "";
			if (jqXHR.responseText) {
				msg =  jqXHR.responseText;
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					msg = "Error : Unable to communicate with the server.";
				}
			}
			$(ADD_MAPPING_ERROR_CONTAINER).text(msg);
		},
		deleteMappingBtnClicked: function(){
			var mappingName = $(DELETE_MAPPING_NAME).text();
			wHandler.deleteMapping(mappingName);
		},
		deleteMappingSuccess: function(){
			pageStatus.mappingsFetched = false;
			_this.fetchMappings();
		},
		deleteMappingError: function(jqXHR){
			var msg = "";
			if (jqXHR.responseText) {
				msg =  jqXHR.responseText;
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					msg = "Error : Unable to communicate with the server.";
				}
			}
			var msgObj={msg:msg, tag:"danger", shortMsg:"Failed to delete mapping."};
			_this.popupNotificationMessage(null,msgObj);
		},
		displaySLAs: function (result){
			$(MAPPING_SLA).empty();
			var keys = result.columnNames;
			var slaNameColIndex = -1;
			$.each(keys, function(k, v) {
				if(v == 'name'){
					slaNameColIndex = k;
				}
			});
			var slas = [];
			if(slaNameColIndex >=0){
				$.each(result.resultArray, function(i, data){
					slas.push(data[slaNameColIndex]);
				});			
			}
			$.each(slas, function(key, value) {   
				$(MAPPING_SLA)
				.append($("<option></option>")
						.attr("value",value)
						.text(value)); 
			});

			var sColIndexName = dataTableColNames.indexOf("sla");

			if($.inArray(mappingDialogParams.data[sColIndexName], slas)){
				$(MAPPING_SLA).val(mappingDialogParams.data[sColIndexName]);
			}
		},
		fetchSLAsError: function (jqXHR) {

		}		
	});


	return WorkloadMappingConfigurationView;
});
