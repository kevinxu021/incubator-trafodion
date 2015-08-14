define([
    'views/BaseView',
    'text!templates/workbench.html',
    'jquery',
    'jit',
    'datatables',
    'datatablesBootStrap',
    'tabletools'
], function (BaseView, WorkbenchT, $) {
    'use strict';

    var setRootNode = false;
    var LOADING_SELECTOR = ".dbmgr-spinner";			
    var st = null;
    var resizeTimer = null;			
    var formDialogContents;
    var formStateView;			
    var GRIDCONTAINER = "#dbmgr-1";
    var oDataTable = null;
    var controlStatements = null;
    var previousScrollTop = 0;
    var controlDialog = null;
    var controlStmts = "";
    var _that = null;
    
    var deselectedButtonStyle = 
    	{
    		"color": "#007dba",
    		"border": "2px solid #BFE5F5",
    		"border-top-right-radius": "5px",
    		"border-bottom-left-radius": "5px",
    		"background-color": "#BFE5F5"
    	};
    var selectedButtonStyle = 
    	{
    		"background-color": "#0096d6",
    		"border-color": "#0096d6",
    		"color": "#fff"
    	};
    
    $jit.ST.Plot.NodeTypes.implement({
    	'nodeline': {
    	  'render': function(node, canvas, animating) {
    			if(animating === 'expand' || animating === 'contract') {
    			  var pos = node.pos.getc(true), nconfig = this.node, data = node.data;
    			  var width  = nconfig.width, height = nconfig.height;
    			  var algnPos = this.getAlignedPos(pos, width, height);
    			  var ctx = canvas.getCtx(), ort = this.config.orientation;
    			  ctx.beginPath();
    			  if(ort == 'left' || ort == 'right') {
    				  ctx.moveTo(algnPos.x, algnPos.y + height / 2);
    				  ctx.lineTo(algnPos.x + width, algnPos.y + height / 2);
    			  } else {
    				  ctx.moveTo(algnPos.x + width / 2, algnPos.y);
    				  ctx.lineTo(algnPos.x + width / 2, algnPos.y + height);
    			  }
    			  ctx.stroke();
    		  } 
    	  }
    	}
    });    			

    var WorkbenchView = BaseView.extend({

        template: _.template(WorkbenchT),

        showLoading: function(){
        	$('#loadingImg').show();
        },

        hideLoading: function () {
        	$('#loadingImg').hide();
        },
        				
        drawExplain: function (jsonData) {
        	_that.hideLoading();
        	$('#text-result-container').show();
        	$('#text-result').text(jsonData.planText);
        	$("#infovis").empty();

        	//init Spacetree
        	//Create a new ST instance
        	st = new $jit.ST({
        		'injectInto': 'infovis',
        		orientation: "top",
        		constrained: false,
        		//set duration for the animation
        		duration: 800,
        		//set animation transition type
        		transition: $jit.Trans.Quart.easeInOut,
        		//set distance between node and its children
        		levelDistance: 40,
        		siblingOffset: 100,
        		width: ($(GRIDCONTAINER).width()),
        		height:($(GRIDCONTAINER).height()),
        		//set max levels to show. Useful when used with
        		//the request method for requesting trees of specific depth
        		levelsToShow: 20,
        		offsetX: -100,
        		offsetY: 350,
        		//set node and edge styles
        		//set overridable=true for styling individual
        		//nodes or edges
        		Node: {
        			height: 20,
        			width: 40,
        			//use a custom
        			//node rendering function
        			type: 'nodeline',
        			color:'#23A4FF',
        			lineWidth: 2,
        			align:"center",
        			overridable: true
        		},
        		Navigation: {  
        			enable: true,  
        			//panning: 'avoid nodes',
        			panning: true,
        			zooming: 20
        		},
        		Edge: {
        			type: 'bezier',
        			lineWidth: 2,
        			color:'#23A4FF',
        			overridable: true
        		},
        		Tips: {  
        			enable: true,  
        			type: 'auto',  
        			offsetX: 20,  
        			offsetY: 20,  
        			onShow: function(tip, node) {  
        			  tip.innerHTML = node.data.formattedCostDesc;  
        			  tip.style.width = 450 + 'px';
        			  tip.className = 'mytooltip';
        			}  
        		  },
        		//Add a request method for requesting on-demand json trees. 
        		//This method gets called when a node
        		//is clicked and its subtree has a smaller depth
        		//than the one specified by the levelsToShow parameter.
        		//In that case a subtree is requested and is added to the dataset.
        		//This method is asynchronous, so you can make an Ajax request for that
        		//subtree and then handle it to the onComplete callback.
        		//Here we just use a client-side tree generator (the getTree function).
        		request: function(nodeId, level, onComplete) {
        		  //var ans = getTree(nodeId, level);
        		  //onComplete.onComplete(nodeId, ans);  
        		},
        		
        		onBeforeCompute: function(node){
        			//Log.write("loading " + node.name);
        		},
        		
        		onAfterCompute: function(){
        			//Log.write("done");
        		},
        		onComplete: function(){   

        			if(!setRootNode){
        			 var m = { 
        						offsetX: st.canvas.translateOffsetX, 
        						offsetY: st.canvas.translateOffsetY 
        				};
        			 st.onClick(st.root, { Move: m});
        			 setRootNode = true;
        			}	
        		},
        		
        		//This method is called on DOM label creation.
        		//Use this method to add event handlers and styles to
        		//your node.
        		onCreateLabel: function(label, node){
        			var nodeName = node.name;
        			nodeName = nodeName.replace("_", " ");
        			nodeName = nodeName.replace("SEABASE","TRAFODION");
        			nodeName = _that.toProperCase(nodeName);
        			
        			var html = nodeName;
        			switch(node.name)
        			{
        				case 'FILE_SCAN':
        				case 'INDEX_SCAN':
        				case 'FILE_SCAN_UNIQUE':
        				case 'INDEX_SCAN_UNIQUE':
        					html = "<img src='img/file_scan.gif' />" + nodeName;
        					break;
        				case 'PARTITION_ACCESS':
        					html =  "<img src='img/partition_scan.gif' />" + nodeName;
        					break;
        				case 'HASH_GROUPBY':
        					html =  "<img src='img/hash_groupby.png' />" + nodeName;
        					break;
        				case 'HASH_PARTIAL_GROUPBY_LEAF':
        					nodeName = "Hash Groupby Leaf";
        					html =  "<img src='img/hash_groupby.png' />" + nodeName;
        					break;
        				case 'HASH_PARTIAL_GROUPBY_ROOT':
        					nodeName = "Hash Groupby Root";
        					html =  "<img src='img/hash_groupby.png' />" + nodeName;
        					break;
        				case 'SHORTCUT_SCALAR_AGRR':
        				case 'SORT_SCALAR_AGGR':
        					nodeName = "Scalar Aggr";
        					html =  "<img src='img/scalar_aggr.png' />" + nodeName;
        					break;
        				case 'SORT':
        				case 'SORT_GROUPBY':
        					html =  "<img src='img/sort_group_by.png' />" + nodeName;
        					break;
        				case 'SORT_PARTIAL_AGGR_LEAF':
        					nodeName = "Sort Aggr Leaf";
        					html =  "<img src='img/sort_group_by.png' />" + nodeName;
        					break;
        				case 'SORT_PARTIAL_AGGR_ROOT':
        					nodeName = "Sort Aggr Root";
        					html =  "<img src='img/sort_group_by.png' />" + nodeName;
        					break;
        				case 'SORT_PARTIAL_GROUPBY_LEAF':
        					nodeName = "Sort Groupby Leaf";
        					html =  "<img src='img/sort_group_by.png' />" + nodeName;
        					break;
        				case 'SORT_PARTIAL_GROUPBY_ROOT':
        					nodeName = "Sort Groupby Root";
        					html =  "<img src='img/sort_group_by.png' />" + nodeName;
        					break;
        				case 'INSERT':
        				case 'INSERT_VSBB':
        					html =  "<img src='img/insert.gif' />" + nodeName;
        					break;
        				case 'PROBE_CACHE':
        					html =  "<img src='img/probe_cache.png' />" + nodeName;
        					break;
        				case 'HYBRID_HASH_ANTI_SEMI_JOIN':
        				case 'HYBRID_HASH_JOIN':
        				case 'HYBRID_HASH_SEMI_JOIN':
        					nodeName = "Hash Join";
        					html =  "<img src='img/hash_join.png' />"  + nodeName;
        					break;
        				case 'LEFT_HYBRID_HASH_JOIN':
        				case 'LEFT_ORDERED_HASH_JOIN':
        					nodeName = "Left Hash Join";
        					html =  "<img src='img/hash_join.png' />"  + nodeName;
        					break;
        				case 'ORDERED_HASH_ANTI_SEMI_JOIN':
        				case 'ORDERED_HASH_JOIN':
        				case 'ORDERED_HASH_SEMI_JOIN':
        					nodeName = "Hash Join";
        					html =  "<img src='img/hash_join.png' />"  + nodeName;
        					break;
        				case 'TUPLE_FLOW':
        					html =  "<img src='img/tuple_flow.png' />"  + nodeName;
        					break;
        				case 'LEFT_MERGE_JOIN':
        				case 'MERGE_ANTI_SEMI_JOIN':
        				case 'MERGE_JOIN':
        				case 'MERGE_SEMI_JOIN':
        					html =  "<img src='img/merge_join.gif' />"  + nodeName;
        					break;									
        				case 'NESTED_ANTI_SEMI_JOIN':
        				case 'LEFT_NESTED_JOIN':
        				case 'NESTED_JOIN':
        				case 'NESTED_SEMI_JOIN':
        					html =  "<img src='img/nested_join.png' />"  + nodeName;
        					break;
        				case 'MERGE_UNION':
        					html =  "<img src='img/merge_union.gif' />" + nodeName;
        					break;
        				case 'ESP_EXCHANGE':
        					html =  "<img src='img/esp_exchange.png' />" + nodeName;
        					break;
        				case 'SPLIT_TOP':
        					html =  "<img src='img/split_top.gif' />" + nodeName;
        					break;
        				case 'HIVE_INSERT':
        				case 'TRAFODION_':
        				case 'TRAFODION_DELETE':
        				case 'TRAFODION_INSERT':
        					html =  "<img src='img/trafodion_insert.png' />" + nodeName;
        					break;
        				case 'HIVE_SCAN':
        				case 'TRAFODION_SCAN':
        				case 'SEABASE_SCAN':
        					html =  "<img src='img/seabase_scan.gif' />" + nodeName;
        					break;
        				case 'ROOT':
        					html =  "<img src='img/root.png'/> " + nodeName;
        					break;
        				default:
        					html =  "<img src='img/undefined.gif' />" + nodeName;
        				break;
        			}
        			label.id = node.id;            
        			label.innerHTML = html;
        			label.onclick = function(){
        				 var m = { 
        							offsetX: st.canvas.translateOffsetX, 
        							offsetY: st.canvas.translateOffsetY 
        					}; 
        				 st.onClick(node.id, { Move: m }); 
        			};
        			//set label styles
        			var style = label.style;
        			style.width = 175 + 'px';
        			style.height = 17 + 'px';            
        			style.cursor = 'pointer';
        			style.color = '#000';
        			style.display = 'inline-table';
        			//style.backgroundColor = '#1a1a1a';
        			style.fontSize = '15px';
        			style.fontWeight = 'bold';
        			//style.textAlign= 'center';
        			//style.textDecoration = 'underline';
        			//style.paddingTop = '3px';
        			style.paddingLeft = '3px';
        		},
        		
        		//This method is called right before plotting
        		//a node. It's useful for changing an individual node
        		//style properties before plotting it.
        		//The data properties prefixed with a dollar
        		//sign will override the global node style properties.
        		onBeforePlotNode: function(node){
        			//add some color to the nodes in the path between the
        			//root node and the selected node.
        			if (node.selected) {
        				node.data.$color = "#ff7";
        			}
        			else {
        				delete node.data.$color;
        			}
        		},
        		
        		//This method is called right before plotting
        		//an edge. It's useful for changing an individual edge
        		//style properties before plotting it.
        		//Edge data proprties prefixed with a dollar sign will
        		//override the Edge global style properties.
        		onBeforePlotLine: function(adj){
        			if (adj.nodeFrom.selected && adj.nodeTo.selected) {
        				adj.data.$color = "#eed";
        				adj.data.$lineWidth = 3;
        			}
        			else {
        				delete adj.data.$color;
        				delete adj.data.$lineWidth;
        			}
        		}
        	});
        	//load json data
        	st.loadJSON(jsonData);
        	//compute node positions and layout
        	st.compute();
        	$("#infovis").show();
        	//emulate a click on the root node.
        	st.onClick(st.root);
        	_that.doResize();
        	//end
        },

        toProperCase: function (s) {
        	return s.toLowerCase().replace(/^(.)|\s(.)/g, function($1) {
        		return $1.toUpperCase();
        	});
        },
        init: function () {
        	_that = this;
        	$('#text-result-container').hide();
        	controlDialog = $("#dialog-form").dialog({
              autoOpen: false,
              height: 300,
              width: 500,
              modal: true,
              buttons: {
                "OK": function (){
        			controlStmts = $("#controlStmts").val();
        			if(controlStmts == null) {
        				controlStmts = "";
        			} else {
        				controlStmts = controlStmts.replace(/(\r\n|\n|\r)/gm,"");
        			}
        			controlDialog.dialog( "close" );
        		},
                Cancel: function() {
                  controlDialog.dialog( "close" );
                }
              },
              close: function() {
                //form[ 0 ].reset();
                //allFields.removeClass( "ui-state-error" );
              }
            });
        	
        	this.hideLoading();
        	//initFilterDialog();
        	$("#explainQuery").on('click', this.explainQuery);
        	$("#executeQuery").on('click', this.executeQuery);
        	$("#setControlStmts").on('click', this.openFilterDialog);
        	$("#infovis").show();
        	$("#errorText").hide();
        	
        	
        },
        resume: function(){
        	
        },
        onRelayout: function () {
        	this.onResize();
        },
        onResize: function () {
        	clearTimeout(resizeTimer);
        	resizeTimer = setTimeout(this.doResize, 200);
        },
        doResize: function () {
        	if(st != null) {
        		st.canvas.resize($('#infovis').width(), ($(GRIDCONTAINER).height() + $(GRIDCONTAINER).scrollTop() + 800));
        	}
        },

        openFilterDialog: function () {
        	controlDialog.dialog( "open" );
        },

        explainQuery: function () {
        	var queryText = $("#query-text").val();
        	if(queryText == null || queryText.length == 0){
        		alert('Query text cannot be empty.');
        		return;
        	}
        	$("#infovis").hide();
        	$("#errorText").hide();
        	$("#query-result-container").hide();
        	var param = {sQuery : queryText, sControlStmts: controlStmts};

        	_that.showLoading();
        	
        	$.ajax({
        	    url:'resources/queries/explain',
        	    type:'POST',
        	    data: JSON.stringify(param),
        	    dataType:"json",
        	    contentType: "application/json;",
        	    success:_that.drawExplain,
        	    error:function(jqXHR, res, error){
        	    	_that.hideLoading();
        	    	_that.showErrorMessage(jqXHR);
        	    }
        	});
            
        },

        executeQuery: function () {
        	var queryText = $("#query-text").val();
        	if(queryText == null || queryText.length == 0){
        		alert('Query text cannot be empty.');
        		return;
        	}
        	_that.showLoading();
        	$("#infovis").hide();
        	$("#errorText").hide();
        	$('#text-result-container').hide();

        	$("#query-result-container").show();
        	var param = {sQuery : queryText, sControlStmts: controlStmts};
        	
        	$.ajax({
        	    url:'resources/queries/execute',
        	    type:'POST',
        	    data: JSON.stringify(param),
        	    dataType:"json",
        	    contentType: "application/json;",
        	    success:_that.displayResults,
        	    error:function(jqXHR, res, error){
        	    	_that.hideLoading();
        	    	_that.showErrorMessage(jqXHR);
        	    }
        	});
        },

        displayResults: function (result){
        	_that.hideLoading();
        	var keys = result.columnNames;
            
        	if(keys != null && keys.length > 0) {
        		var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="query-results"></table>';
        		$('#query-result-container').html( sb );
        		
        		var aoColumns = [];
        		var aaData = [];
        		
        		$.each(result.resultArray, function(i, data){
        		  aaData.push(data);
        		});

        		// add needed columns
        		$.each(keys, function(k, v) {
        			var obj = new Object();
        			obj.title = v;
        			aoColumns.push(obj);
        		});
        		
        		var bPaging = aaData.length > 25;
        		
        		 $('#query-results').dataTable({
        			 dom: 'T<"clear">lfrtip',
        			"bProcessing": true,
        			"bPaginate" : true, 
        			"iDisplayLength" : 25, 
        			"sPaginationType": "simple_numbers",
    		        "scrollCollapse": true,
        			"aaData": aaData, 
        			"aoColumns" : aoColumns,
        			paging: true,
					"tableTools": {
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					}
        		 });
        	 }

        },
        
        displayResultsOld: function (result){
        	_that.hideLoading();
        	var keys;
            $.each(result, function(i, data){
              keys = Object.keys(data);
        	});
            
        	if(keys != null && keys.length > 0) {
        		var sb = '<table class="table table-striped table-bordered table-hover" id="query-results"></table>';
        		$('#query-result-container').html( sb );
        		
        		var aoColumns = [];
        		var aaData = [];
        		
        		$.each(result, function(i, data){
        		var rowData = [];
        		  $.each(keys, function(k, v) {
        			rowData.push(data[v]);
        		  });
        		  aaData.push(rowData);
        		});

        		// add needed columns
        		$.each(keys, function(k, v) {
        			var obj = new Object();
        			obj.title = v;
        			aoColumns.push(obj);
        		});
        		
        		var bPaging = aaData.length > 25;

        		oDataTable = $('#query-results').dataTable({
 					dom: 'T<"clear">lfrtip',
        			"bProcessing": true,
        			"bPaginate" : true, 
        			"iDisplayLength" : 25, 
        			"sPaginationType": "simple_numbers",
    		        "scrollCollapse": true,
        			"aaData": aaData, 
        			"aoColumns" : aoColumns,
        			paging: true,
					"tableTools": {
						"sSwfPath": "../bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					}
        		 });
        	 }
        },

        showErrorMessage: function (jqXHR) {
        	_that.hideLoading();
        	$("#infovis").hide();
        	$("#query-result-container").hide();
        	$('#text-result-container').hide();

        	$("#errorText").show();
        	if (jqXHR.responseText) {
        		$("#errorText").text(jqXHR.responseText);
        	}
        }        
    });

    return WorkbenchView;
});
