// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'jquery',
        'underscore',
        'backbone',
        'common',
        'handlers/ServerHandler',
        'bootstrapNotify',
        'jqueryui'
        ], function ($, _, Backbone, common, serverHandler) {
	'use strict';
    var _this = null;
    var __this=null;
	var BaseView = Backbone.View.extend({

		el: $('#wrapper'),

		initialized: false,
		
		TASK_MENU:'#notify',

		pageWrapper: null,
		
		pageIdentifier:null,
		
		currentURL:null,
		
		redirectFlag:null,
		/*initialize: function () {

		},*/
    	refreshTimer: (function() {
			var _interval;
			var _intervalId;
			var _refreshTimer = {};
			
			_refreshTimer.set = function(newInterval) {
				if(newInterval){
					if(typeof newInterval === 'string'){
						var value = parseFloat(newInterval);
						if(!isNaN(value)) {
							_interval = value * 60 * 1000;
						}
					}else{
						_interval = newInterval * 60 * 1000;
					}
					
					if(_interval != 0){
						_refreshTimer.start();
					}else{
						_refreshTimer.stop();
					}
				}else{
					_refreshTimer.stop();
				}

			};
			
			_refreshTimer.get = function() {
				return _interval;
			};
			
			_refreshTimer.beep = function() {
				// console.log("========beep==========");
				if(_this.onTimerBeeped){
					_this.onTimerBeeped();
				}
			};
			
			_refreshTimer.start = function() {
				_refreshTimer.stop();
				if (_interval) {
					_intervalId = window.setInterval(_refreshTimer.beep, _interval);
				}
			};
			
			_refreshTimer.stop = function() {
				if (_intervalId) {
					window.clearInterval(_intervalId);
				}
			};
			
			return _refreshTimer;
		}()),
		
		initialize: function(options) { 
			_.bindAll(this, 'beforeRender', 'render', 'afterRender'); 
			_this = this; 
			
			this.render = _.wrap(this.render, function(render, args) { 
				_this.beforeRender(); 
				render(args); 
				_this.afterRender(); 
				return _this; 
			}); 
		}, 

		beforeRender: function() { 
		}, 

		render: function (args) {
			if(this.pageWrapper == null){
				this.$el.html(this.template);
				if(common.serverTimeZone == null){
					serverHandler.loadServerConfig();
				}
				this.init(args);
			}else
			{
				//this.pause();
				this.$el.empty().append(this.pageWrapper);
				this.resume(args);
			}
			return this;        	
		},
		
		afterRender: function() { 
		},

		init: function(args){
			__this=this;
			common.on(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
			if(this.doInit){
				this.doInit(args);
				this.currentURL=window.location.hash;
			}
			$('#notifyMenu').on('shown.bs.dropdown', function(){
				$('#notification-btn').find('.dbmgr-notify-icon').remove();
			});
		},
		
		resume: function(args){
			if(this.doResume){
				this.doResume(args);
				this.currentURL=window.location.hash;
			}
			common.on(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
		},
		pause: function() {
			if(this.doPause){
				this.doPause();
			}
			common.off(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
		},
		flashBackground:function(){
			$("#notifyMenu>a").attr("style","background-color:yellow");
			setTimeout(function(){$("#notifyMenu>a").attr("style","background-color:transparent");},500);
		},
		refreshTimeDiff:function(){
			var current = new Date();
			for(var i=0;i<common.MESSAGE_LIST.length;i++){
				var timeAgo = common.toTimeDifferenceFromLocalDate(common.MESSAGE_LIST[i].time,current);
				$("span.timeAgo").eq(i).text(timeAgo);
			}
		},
		collectNewNotifyMessage:function(obj){
			$("#notifyMenu>ul>div").remove();
			var notifyIndicator = $('#notification-btn').find('.dbmgr-notify-icon');
			if(notifyIndicator.length == 0){
				$('#notification-btn').append('<i class="dbmgr-notify-icon fa fa-exclamation-circle fa-stack-1x" style="color:yellow"></i>');
			}
			
			common.MESSAGE_COUNT++;
			var currentTime=new Date();
			var interval=setInterval(__this.flashBackground,1000);
			setInterval(__this.refreshTimeDiff,60000);
			setTimeout(function(){clearInterval(interval)},5000);
			//remove the empty li.
			if($("#notifyMenu>ul>li>a>strong").text().trim()=='No available notifications'){
				$("#notifyMenu>ul>li").remove();
			}
			//sometimes the response will be undefined.
			if(common.MESSAGE_COUNT>4){
				if(common.isAllNotificationInserted==false){
					common.isAllNotificationInserted=true;
					$("#notifyMenu>ul").append('<li id="allNotification"><a class="text-center active"><strong>See All Notifications</strong><i class="fa fa-angle-right"></i></a></li>');
				}
			}
			if(obj.msg == undefined){
				obj.msg="there is no response for current request.";
				obj.shortMsg="there is no response.";
			}
			var alertClass = "alert-success fa-check-circle";
			if(obj.tag == "danger"){
				alertClass = "alert-danger fa-times-circle";
			}else if(obj.tag == "warning"){
				alertClass = "alert-warning fa-warning";

			}

			//Compute the hash string on the url.
			var hs = common.hashString(obj.url ? obj.url : "");
			
			if(obj.lastMessageOnly && obj.lastMessageOnly == true){
				
				//delete all other earlier messages that match obj.url
				$.each(common.MESSAGE_LIST, function (index , value){
					if(obj.url == value.url){
						common.MESSAGE_LIST.splice(index, 1);
						common.MESSAGE_COUNT --;
					}
				});
				
				//Delete the older messages from the UI, by matching the hash string
				var itemList = $('#notifyMenu>ul>li');
				if(itemList.length > 0){
					for(var i=0;i<itemList.length;i++){
						if($(itemList[i]).attr("hashstr") && $(itemList[i]).attr("hashstr") == hs){
							$(itemList[i]).remove();
							if($(itemList[i+1]).hasClass("divider")){
								$(itemList[i+1]).remove();
								i++;								
							}
						}
					}
				}
			}
			
			$("#notifyMenu>ul").prepend('<li hashstr="'+hs+'"><a class="active"><div class="notifyDetail"><i class="fa '+ alertClass + ' fa-fw"></i><i style="padding-left:2px">'+ obj.shortMsg.substr(0,35)+'</i> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span><button type="button" aria-hidden="true" class="pull-right close" data-notify="dismiss" style="color: black;">×</button></div></a></li><li class="divider"></li>');

			common.MESSAGE_LIST.splice(0,0,{msg:obj.msg,tag:obj.tag,url:obj.url,time:currentTime});
			common.hideNotifications=$("#notifyMenu>ul>li[id!='allNotification']:gt(7)");
			$(".notifyDetail").unbind().on("click",__this.popupNotificationMessage);
			$(".close").unbind().on("click",__this.removeNotificationMessage);
			if($("#allNotification").text().indexOf("All")>0){
				//showed part of notifications
				common.hideNotifications.hide();
				$("#allNotification").unbind().on("click",__this.showAllNotifications);
			}else if($("#allNotification").text().indexOf("Part")>0){
				//showed all of notifications
				$("#allNotification").unbind().on("click",__this.showPartOfNotifications);
				if(common.MESSAGE_COUNT>7){
					$("#notifyMenu>ul").attr("style","overflow:scroll;height:400px;width:450px");
				}
			}
			$("#notifyMenu>ul").prepend('<div class="panel-heading" style="text-align: center;border: 1px solid #e5e5e5;padding-top: 5px;color: #7b8a8b;">Total Count '+common.MESSAGE_COUNT+'</div>');
		},
		showAllNotifications:function(event){
			$("#notifyMenu>ul>li").show();
			$("#allNotification>a>strong").text("See Part of Notifications");
			$("#allNotification").unbind().on("click",__this.showPartOfNotifications);
			//add scroll bar style whie more than 7 notifications
			if(common.MESSAGE_COUNT>7){
				$("#notifyMenu>ul").attr("style","overflow:scroll;height:400px;width:450px");
			}
			event.stopPropagation();
		},
		showPartOfNotifications:function(event){
			$("#notifyMenu>ul").attr("style","width:450px");
			common.hideNotifications.hide();
			$("#allNotification>a>strong").text("See All Notifications");
			$("#allNotification").unbind().on("click",__this.showAllNotifications);
			event.stopPropagation();
		},
		removeNotificationMessage:function(event,index){
			var i;
			if($.type(event)=="object"){
				//triggered by the panel x mark.
				i=$(this).closest("li").index();
			}/*else{
				//triggered by the popup x mark waiting for plugin bower download available of onclick function..
				i=index;
			}*/
			common.MESSAGE_COUNT--;
			//remove the scroll bar whie less than 7
			if(common.MESSAGE_COUNT<=7){
				$("#notifyMenu>ul").attr("style","width:450px");
			}
			$("#notifyMenu>ul>div").text("Total Count " + common.MESSAGE_COUNT);
			$("#notifyMenu>ul>li").eq(i-1).remove();
			$("#notifyMenu>ul>li").eq(i-1).remove();
			common.MESSAGE_LIST.splice((i-1)/2,1);
			//if the removed element is display:block, redefine the hide and show notifications
			if(this.parentElement.style.display==""){
				__this.redefineShowHideNotifications();
			}
			//while all the notifications are removed.
			if($("#notifyMenu>ul>li [class!='divider']").length==0){
				$("#notifyMenu>ul>div").remove();
				$("#notifyMenu>ul").prepend('<li><a class="text-center active"><i class="fa fa-angle-left"></i><strong>No available notifications</strong><i class="fa fa-angle-right"></i></a></li>');
			}
			event.stopPropagation();
		},
		redefineShowHideNotifications:function(){
			var showList=$("#notifyMenu>ul>li[id!='allNotification']:lt(8)");
			//get the four show elements
			showList.show();
			if(common.MESSAGE_COUNT<=4){
				$("#allNotification").remove();
				common.isAllNotificationInserted=false;
			}
		},
		popupNotificationMessage:function(event,obj){
			if(event!=null){
				//this for redirect case
				var i=$(this).closest("li").index();
				event.stopPropagation();
				//used to remove the notification from popup.
				common.popupIndex=(i-1)/2;
				obj=common.MESSAGE_LIST[(i-1)/2];
			}
			//this is for no redirect
			if(obj.msg==undefined){
				obj.msg="Operation failed.";
			}
			$.notify({message: obj.msg,
				url:obj.url,
				target:'_self'},
				{
					// settings
					element: 'body',
					type:obj.tag,
					z_index:1000,
					delay:10000,
					timer:1000,
					/*onClose:__this.removeNotificationFromPopup,*/
					placement: {
						from: 'bottom',
						align: 'right'
					}});
		},
		removeNotificationFromPopup:function(){
			/*__this.removeNotificationMessage(common.popupIndex*2);*/
		},
		remove: function(){
			_this.refreshTimer.stop();
			this.pause();
			var childElement = $(this.$el[0]).find('#page-wrapper');
			if(childElement && childElement.length > 0)
				this.pageWrapper = childElement.detach();

		},
		
		reset: function(){
			if(this.doReset){
				this.doReset();
			}
		}
	});

	return BaseView;
});
