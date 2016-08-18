//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workload_profiles.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'handlers/ServerHandler',
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
        ], function (BaseView, WorkloadsT, $, wHandler, sHandler, moment, common) {
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
	HOST_SELECTION_MODE = '#host-selection-mode',
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
	var nodesList = null;
	var nodesDataTable = null;
	var allNodesSelectState = false;

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
			sHandler.on(sHandler.FETCHDCS_SUCCESS, this.fetchNodesSuccess);
			sHandler.on(sHandler.FETCHDCS_ERROR, this.fetchNodesError); 

			$(ADD_PROFILE_BTN).on('click', this.addProfileBtnClicked);
			$(DELETE_PROFILE_YES_BTN).on('click', this.deleteProfileBtnClicked);
			$(PROFILE_APPLY_BTN).on('click', this.profileApplyBtnClicked);
			$(PROFILE_RESET_BTN).on('click', this.profileResetBtnClicked);

			this.fetchNodes();

			$.validator.addMethod("wmsprofile_alphanumeric", function(value, element) {
				if(profileDialogParams.type && profileDialogParams.type == 'alter')
					return true; // For alter we don't allow editing the name,so no check needed
				
				return this.optional(element) || /^\w+$/i.test(value);
			}, "Only alphanumeric characters and underscores are allowed");

			$.validator.addMethod("cqdssets", function(value, element) {
				return this.optional(element) || /^[\w\-;_ '"\r\n]*$/i.test(value);
			}, "Only alphanumeric characters, underscores, semicolons, spaces, newline and single/double quotes are allowed");

			profileFormValidator = $(PROFILE_FORM).validate({
				rules: {
					"profile_name": { required: true, wmsprofile_alphanumeric: true},
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
			sHandler.on(sHandler.FETCHDCS_SUCCESS, this.fetchNodesSuccess);
			sHandler.on(sHandler.FETCHDCS_ERROR, this.fetchNodesError); 
			this.fetchNodes();
		},
		doPause: function(){
			$(REFRESH_MENU).off('click', this.doRefresh);
			$(window).off('resize', this.onResize);
			sHandler.off(sHandler.FETCHDCS_SUCCESS, this.fetchNodesSuccess);
			sHandler.off(sHandler.FETCHDCS_ERROR, this.fetchNodesError); 
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
		fetchNodes:function(){
			sHandler.fetchDcsServers(false);
		},
		fetchNodesSuccess: function(result) {
			var nList = [];
			$.each(result.resultArray, function(i, v){
				if($.inArray(v[0], nList) < 0){
				nList.push(v[0]);
				}
			});
			_this.nodesList = nList;
		},
		fetchNodesError: function(jqXHR){
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

					$('#wprofile-form input, select, textarea, table').prop('disabled', false);
					$(PROFILE_APPLY_BTN).attr('disabled', false);
					$(PROFILE_RESET_BTN).attr('disabled', false);
					
					$(PROFILE_DIALOG_TITLE).text('Add Profile');
					$(PROFILE_NAME).val("");
					var cqds = profileDialogParams.data["cqd"].replace(/<br>/g,"\n"); 
					$(CQD_CONTAINER).val(cqds);
					var sets = profileDialogParams.data["set"].replace(/<br>/g,"\n"); 
					$(SET_CONTAINER).val(sets);
					$(PROFILE_NODES).val(profileDialogParams.data["hostList"]);
					var selMode = profileDialogParams.data["hostSelectionMode"];
					$(HOST_SELECTION_MODE).val(selMode != null ? selMode.toLowerCase() : "");
					_this.setNodeSelection();
				}
				if(profileDialogParams.type && profileDialogParams.type == 'alter'){
					if(profileDialogParams.data["isDefault"] == 'yes'){
						$('#wprofile-form input, select, textarea, table').prop('disabled', true);
						$(PROFILE_APPLY_BTN).attr('disabled', true);
						$(PROFILE_RESET_BTN).attr('disabled', true);
					}else{
						$('#wprofile-form input, select, textarea, table').prop('disabled', false);
						$(PROFILE_APPLY_BTN).attr('disabled', false);
						$(PROFILE_RESET_BTN).attr('disabled', false);
					}					
					$(PROFILE_DIALOG_TITLE).text('Alter Profile');
					$(PROFILE_NAME).attr('disabled', true);
					$(PROFILE_NAME).val(profileDialogParams.data["name"]);
					var cqds = profileDialogParams.data["cqd"].replace(/<br>/g,"\n"); 
					$(CQD_CONTAINER).val(cqds);
					var sets = profileDialogParams.data["set"].replace(/<br>/g,"\n"); 
					$(SET_CONTAINER).val(sets);
					$(PROFILE_NODES).val(profileDialogParams.data["hostList"]);
					var selMode = profileDialogParams.data["hostSelectionMode"];
					$(HOST_SELECTION_MODE).val(selMode != null ? selMode.toLowerCase() : "");
					_this.setNodeSelection();
				}
			}			
		},
		selectAll: function(){
			var selectedNodes = [];
			var allNodes = nodesDataTable.column(1).data();
			var cells = nodesDataTable.cells().nodes();
			var selCells = $( cells ).find(':checkbox');
			allNodesSelectState = !allNodesSelectState;
			$.each(selCells, function(i,v){
				$(v).prop('checked', allNodesSelectState);
			});		
		},
		setNodeSelection: function(){
			//_this.nodesList=["node1","node2","node3","node4","node5","node6","node7","node8","node9","node10","node11","node12"]
			var aoColumns = [];// [{"title":"Select"},{"title":"Node Name"}];
			var aaData = [];
			var nodes = profileDialogParams.data["hostList"].replace(/\s+/g, "");
			var nodes = nodes.split(',');
			if(profileDialogParams.data["isDefault"] == 'no'){
				aoColumns.push({"title":"Select"});
			}
			aoColumns.push({"title":"Node Name"});

			$.each(_this.nodesList, function(i, v){
				var data = [];
					if(profileDialogParams.data["isDefault"] == 'no'){
					if($.inArray(v, nodes) > -1){
						data.push(1);
					}else{
						data.push(0);
					}
					data.push(v);
				}else{
					if($.inArray(v, nodes) > -1){
						data.push(v);
					}
				}
				if(data.length > 0)
					aaData.push(data);
			});
			
			var aoColumnDefs = [];
			if(profileDialogParams.data["isDefault"] == 'no'){
				aoColumnDefs.push({
					"aTargets": [ 0 ],
					"mData": 0,
					"mRender": function ( data, type, full ) {
						if(type == 'display') {
							if (data == "1") {
								return '<input type=\"checkbox\" checked value="' + data + '">';
							} else {
								return '<input type=\"checkbox\" value="' + data + '">';
							}                       
						}else { 
							return data;
						}
					}
				});
			}
			
			var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="p-nodes-list"></table>';
			$('#pnode-list-container').html( sb );
			var bPaging = aaData.length > 10;
			nodesDataTable = $('#p-nodes-list').DataTable({
				"oLanguage": {
					"sEmptyTable": "There are no nodes"
				},
				dom: '<"top"l<"select-all-button">f>t<"bottom"rip>',
				processing: true,
				paging: bPaging,
				autoWidth: true,
				"iDisplayLength" : 10, 
				"sPaginationType": "full_numbers",
				"aaData": aaData, 
				"aoColumns" : aoColumns,
				"aoColumnDefs" : aoColumnDefs,
				"sScrollY":"200",
				"scrollCollapse": true,
				"order": [[ 0, "desc" ]],
				buttons: []
			});
			if(profileDialogParams.data["isDefault"] == 'no'){
				$('div.select-all-button').html('<button type="button">Select/Unselect All</button>');
				$('div.select-all-button').on('click', this.selectAll);
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
				var cqdColIndex = -1;
				var setColIndex = -1;
				var isDefColIndex = -1;
				var selModeColIndex = -1;
				
				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = common.UpperCaseFirst(v);
					if(v == 'name'){
						profileNameColIndex = k;
					}
					if(v == 'lastUpdate'){
						updateTimeColIndex = k;
						obj.title = 'Last Update Time';
					}
					if(v == 'cqd'){
						cqdColIndex = k;
						obj.title = 'CQDs';
					}
					if(v == 'set'){
						setColIndex = k;
						obj.title = 'SETs';
					}
					if(v == 'isDefault'){
						isDefColIndex = k;
					}
					if(v == 'hostList'){
						obj.title = 'Hosts';
					}
					if(v == 'hostSelectionMode'){
						obj.title = 'Selection Mode';
						selModeColIndex = k;
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
				if(cqdColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ cqdColIndex ],
						"mData": cqdColIndex,
						"mRender": function ( data, type, full ) {
							if(type == 'display') {
								return data.replace(/<br>/g, "");                         
							}else { 
								return data;
							}
						}
					});					
				}
				if(setColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ setColIndex ],
						"mData": setColIndex,
						"mRender": function ( data, type, full ) {
							if(type == 'display') {
								return data.replace(/<br>/g, "");                         
							}else { 
								return data;
							}
						}
					});					
				}
				
				//profileDialogParams.data["cqd"].replace(/;/g, ";\n")
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
				
				if(isDefColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [isDefColIndex ],
						"mData": isDefColIndex,
						"visible" : false,
						"searchable" : false
					});					
				}
				if(selModeColIndex >=0){
					aoColumnDefs.push({
						"aTargets": [ selModeColIndex ],
						"mData": selModeColIndex,
						"mRender": function ( data, type, full ) {
							if(data != null){
								return common.toProperCase(data);
							}else 
								return data;
						}
					});
				}
				aoColumnDefs.push({
					"aTargets": [ deleteProfileIconColIndex ],
					"mData": deleteProfileIconColIndex,
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
									if(data[isDefColIndex] == 'no'){
										$(DELETE_PROFILE_NAME).text(data[profileNameColIndex]);
										$(PROFILE_DELETE_DIALOG).modal('show');
									}
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
					paramDataRow[dataTableColNames.indexOf("isDefault")] = 'no';
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
			profile.action = profileDialogParams.type;
			profile.name = $(PROFILE_NAME).val();
			profile.cqds = $(CQD_CONTAINER).val();
			profile.sets = $(SET_CONTAINER).val();
			profile.hostSelectionMode = $(HOST_SELECTION_MODE).val();

			var selectedNodes = [];
			var allNodes = nodesDataTable.column(1).data();
			var cells = nodesDataTable.cells().nodes();
			var selCells = $( cells ).find(':checkbox');
			$.each(selCells, function(i,v){
				if($(v).prop('checked')){
					selectedNodes.push(allNodes[i]);
				}
			});
			profile.nodes = selectedNodes.join(',');
			$(PROFILE_DIALOG_SPINNER).show();
			$(PROFILE_APPLY_BTN).prop("disabled", true);
			$(PROFILE_RESET_BTN).prop("disabled", true);
			wHandler.addAlterProfile(profile);
		},
		profileResetBtnClicked: function(){
			_this.doReset();
			profileFormValidator.resetForm();
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
				msg =  jqXHR.responseText;
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
				msg =  jqXHR.responseText;
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
