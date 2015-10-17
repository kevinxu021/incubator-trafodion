// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['moment',
        'momenttimezone',
        'jquery',
        'jit'
        ],
function(moment, momenttimezone, $) {
	"use strict";

	return (function() {

		function Common() {

			// var _isoDateFormat='yyyy-MM-dd HH:mm:ss'
			this.ISODateFormat = 'YYYY-MM-DD HH:mm:ss';
			var _this = this;
			this.serverTimeZone = null;
			this.serverUtcOffset = 0;
			this.dcsMasterInfoUri = "";
			
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

			this.formatNumberWithCommas = function(number) {
				if(number == null)
					return "";
				return number.toString().replace(/,/g, "").replace(/\B(?=(\d{3})+(?!\d))/g, ",");
			};

			this.toUTCFromMilliSeconds = function(milliSeconds) {
				if (milliSeconds != null) {
					return moment(milliSeconds).utc().format(_this.ISODateFormat);
				}
				return "";
			},
			
			this.toServerLocalFromUTCMilliSeconds = function(utcMsec){
				return utcMsec + _this.serverUtcOffset;
			},
			
			this.toDateFromMilliSeconds = function(milliSeconds) {
				if (milliSeconds != null) {
					return moment(milliSeconds).format(_this.ISODateFormat);
				}
				return "";
			},
			
			this.formatGraphDateLabels = function(utcMilliseconds, interval){
				var offSetString = 'HH:mm';
				
				if (interval <= (1 * 60 * 60 * 1000)) {
					offSetString = 'HH:mm'; // For 1 hour use all data, which is by default every 30 seconds
				} else if (interval <= (6 * 60 * 60 * 1000)) {
					offSetString = 'HH:mm'; // For 6 hours, use every 2 min
				} else if (interval <= (1 * 24 * 60 * 60 * 1000)) {
					offSetString = 'HH:mm'; // For 1 day, use every 15 min
				} else if (interval <= (3 * 24 * 60 * 60 * 1000)) {
					offSetString = 'HH:mm'; // For 3 days, use every 1 hour
				} else if (interval <= (1 * 7 * 24 * 60 * 60 * 1000)) {
					offSetString = 'ddd HH:mm'; // For 1 week, use every 2 hours
				} else if (interval <= (2 * 7 * 24 * 60 * 60 * 1000)) {
					offSetString = 'ddd HH:mm'; // For 2 weeks, use every 4 hours
				} else if (interval <= 1 * 31 * 24 * 60 * 60 * 1000) {
					offSetString = 'ddd HH:mm'; // For 1 month use every 12 hours
				} else if (interval <=  3 * 31 * 24 * 60 * 60 * 1000) {
					offSetString = 'MM-DD HH:mm'; // For 3 months use every 1 day
				} else {
					offSetString =  'MM-DD HH:mm'; // For longer than 3 months, use every 1 week
				}
				return _this.toServerLocalDateFromUtcMilliSeconds(utcMilliseconds, offSetString);
			},
			
			this.toServerLocalDateFromUtcMilliSeconds = function(utcMilliSeconds, formatString) {
				if (utcMilliSeconds != null) {
					//return moment(utcMilliSeconds + (_this.serverUtcOffset)).local().format('YYYY-MM-DD HH:mm:ss');
					if(formatString == null){
						formatString = 'YYYY-MM-DD HH:mm:ss z';
					}
					return moment(utcMilliSeconds + (_this.serverUtcOffset)).tz(_this.serverTimeZone).format(formatString);
				}
				return "";
			},
			this.getTimeZoneOffset = function(localeTimeZone) {
				return moment().tz(localeTimeZone).zone() * 60 * 1000;
			},

			this.getBrowserTimeZoneOffset = function() {
				return moment().zone() * 60 * 1000;
			},

			this.bytesToSize = function(bytes) {
				var units = [ 'bytes', 'KB', 'MB', 'GB', 'TB' ];
				if (bytes <= 0)
					return bytes;

				if (bytes < 1)
					return bytes + ' bytes';

				if (bytes == 1)
					return bytes + ' byte';

				if (bytes <= 1)
					return bytes + ' bytes';
				var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
				if (i <= 0)
					return bytes.toFixed(1) + ' ' + units[0];
				return (bytes / Math.pow(1024, i)).toFixed(1) + units[i];
			};

			this.convertToMB = function(bytes){
				if(bytes <=0)
					return 0;
				return (bytes/1024/1024).toFixed(2);
			};
			
			this.convertToKB = function(bytes){
				if(bytes <=0)
					return 0;
				return (bytes/1024).toFixed(2);
			};

			this.storeSessionProperties = function(timeZone, utcOffset, dcsMasterUri){
				_this.serverTimeZone = timeZone;
				_this.serverUtcOffset = utcOffset;
				_this.dcsMasterInfoUri = dcsMasterUri;
			};
			
			this.convertUnitFromByte = function(bytes) {
				var memory = {};
				var units = [ 'bytes', 'KB', 'MB', 'GB', 'TB' ];

				if (bytes < 1)
				{
					memory.size =  bytes;
					memory.unit = units[0];
					return memory;
				}

				if (bytes == 1)
				{
					memory.size = bytes;
					memory.unit = units[0];
					return memory;
				}

				var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
				if (i == 0)
				{
					memory.size = bytes
					memory.unit = units[0];
					return memory;
				}
				memory.size = parseFloat((bytes / Math.pow(1024, i)).toFixed(0));
				memory.unit = units[i];
				return memory;
			};

			this.bytesToMB = function(bytes) {
				var sizes = [ 'bytes', 'KB', 'MB' ];
				if (bytes <= 0)
					return bytes;
				if (bytes < 1)
					return bytes + ' bytes';
				if (bytes == 1)
					return bytes + ' byte';
				var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
				if (i == 0)
					return _this.formatNumberWithCommas(bytes) + ' ' + sizes[i];
				if(i <= 2)
					return _this.formatNumberWithCommas((bytes / Math.pow(1024, i)).toFixed(1)) +  sizes[i];
				else 
					return _this.formatNumberWithCommas((bytes / Math.pow(1024, 2)).toFixed(1)) +  sizes[2];
			};
			
			this.toProperCase = function(s) {
				return s.toLowerCase().replace(/^(.)|\s(.)/g, function($1) {
					return $1.toUpperCase();
				});
			};
			
			this.millisecondsToString = function(milliseconds) {
				var oneDay = (3600000 * 24);
				var oneHour = 3600000;
				var oneMinute = 60000;
				var oneSecond = 1000;
				var seconds = 0;
				var minutes = 0;
				var hours = 0;
				var days = 0;
				var result;

				if (milliseconds >= oneDay) {
					days = Math.floor(milliseconds / oneDay);
				}

				if (milliseconds >= oneHour) {
					hours = Math.floor(milliseconds / oneHour);
				}

				milliseconds = hours > 0 ? (milliseconds - hours * oneHour) : milliseconds;

				if (milliseconds >= oneMinute) {
					minutes = Math.floor(milliseconds / oneMinute);
				}

				milliseconds = minutes > 0 ? (milliseconds - minutes * oneMinute) : milliseconds;

				if (milliseconds >= oneSecond) {
					seconds = Math.floor(milliseconds / oneSecond);
				}

				milliseconds = seconds > 0 ? (milliseconds - seconds * oneSecond) : milliseconds;

				if (days > 0) {
					result = (days > 9 ? days : "0" + days) + " d ";
				} else {
					result = "";
				}

				if (hours > 0) {
					result = (hours > 9 ? hours : "0" + hours) + ":";
				} else {
					result = "00:";
				}

				if (minutes > 0) {
					result += (minutes > 9 ? minutes : "0" + minutes) + ":";
				} else {
					result += "00:";
				}

				if (seconds > 0) {
					result += (seconds > 9 ? seconds : "0" + seconds);
				} else {
					result += "00";
				}

				return result;
			};
			
			this.generateExplainTree = function(jsonData, setRootNode, onClickCallback){
	        	var st = new $jit.ST({
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
	        		//set max levels to show. Useful when used with
	        		//the request method for requesting trees of specific depth
	        		levelsToShow: 20,
	        		offsetX: -100,
	        		offsetY: 350,
	        		width: $('#dbmgr-1').width(),
	        		height: $('#dbmgr-1').height(),	        		
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
	        			enable: onClickCallback == null,  
	        			type: 'auto',  
	        			//offsetX: 20,  
	        			//offsetY: 20,  
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
	        			nodeName = _this.toProperCase(nodeName);
	        			
	        			var html = nodeName;
	        			switch(node.name)
	        			{
	        				case 'FILE_SCAN':
	        				case 'INDEX_SCAN':
	        				case 'FILE_SCAN_UNIQUE':
	        				case 'INDEX_SCAN_UNIQUE':
	        					html = "<img src='img/file_scan.png' />" + nodeName;
	        					break;
	        				case 'PARTITION_ACCESS':
	        					html =  "<img src='img/partition_scan.png' />" + nodeName;
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
	        					html =  "<img src='img/insert.png' />" + nodeName;
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
	        					html =  "<img src='img/merge_join.png' />"  + nodeName;
	        					break;									
	        				case 'NESTED_ANTI_SEMI_JOIN':
	        				case 'LEFT_NESTED_JOIN':
	        				case 'NESTED_JOIN':
	        				case 'NESTED_SEMI_JOIN':
	        					html =  "<img src='img/nested_join.png' />"  + nodeName;
	        					break;
	        				case 'MERGE_UNION':
	        					html =  "<img src='img/merge_union.png' />" + nodeName;
	        					break;
	        				case 'ESP_EXCHANGE':
	        					html =  "<img src='img/esp_exchange.png' />" + nodeName;
	        					break;
	        				case 'SPLIT_TOP':
	        					html =  "<img src='img/split_top.png' />" + nodeName;
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
	        					html =  "<img src='img/seabase_scan.png' />" + nodeName;
	        					break;
	        				case 'ROOT':
	        					html =  "<img src='img/root.png'/> " + nodeName;
	        					break;
	        				default:
	        					html =  "<img src='img/undefined.png' />" + nodeName;
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
	        				 if(onClickCallback){
	        					 onClickCallback(node.name, node.data.formattedCostDesc);
	        				 }
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
	        	return st;
			};
		}
		return new Common();
	})();

});
