//------------------------------------------------------------------
//
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/shm.h>
#include <sys/time.h>

#include "SCMVersHelp.h"

#include "seabed/fserr.h"
#include "seabed/ms.h"
#include "seabed/pctl.h"
#include "seabed/pevents.h"
#include "seabed/timer.h"
#include "seabed/thread.h"
#include "idtmsrv.h"

short          gv_tleid;
short          gv_time_refresh_delay; // time in tics (10ms)
char           ga_name[BUFSIZ];
char          *gp_shm;
unsigned long *gp_shml;
bool           gv_shook   = false;
bool           gv_verbose = false;

DEFINE_EXTERN_COMP_DOVERS(idtmsrv)

// forwards
void do_reply(BMS_SRE *pp_sre, char *pp_reply, int pv_len, short pv_ec);


//
// Reset the global time counter
//
void reset_time_counter() {

    struct timespec lv_new_ts;

    clock_gettime(CLOCK_REALTIME, &lv_new_ts);
    unsigned long lv_new_tsl = ((unsigned long) lv_new_ts.tv_sec << 20) |
                               ((unsigned long) lv_new_ts.tv_nsec / 1000);

    if (gv_verbose)
        printf("srv: reset_time_counter, original ts =0x%lx,0x%lx converted time 0x%lx\n", lv_new_ts.tv_sec, lv_new_ts.tv_nsec, lv_new_tsl);

    unsigned long lv_existing_tsl = __sync_add_and_fetch_8(gp_shm, 0);
    __sync_add_and_fetch_8(gp_shm, lv_new_tsl - lv_existing_tsl);

    if (gv_verbose)
        printf("srv: reset_time_counter, adjustment=0x%lx, shm=0x%lx\n", lv_new_tsl - lv_existing_tsl, *gp_shml);
}

//
// Convert a time id generated by us back to a timespec
//
struct timespec long_to_timespec(unsigned long pv_time_id) {

    if (gv_verbose)
        printf("srv: long_to_timespec, current pv_time_id Ox%lx\n", pv_time_id);

    struct timespec lv_orig_ts;

    // To restore the original timespec we get the usecs by taking the bottom 20 bits
    // lv_curr_ts.tv_nsec 00000000000000000000000000000000000000000000zzzzzzzzzzzzzzzzzzzz
    lv_orig_ts.tv_nsec = (unsigned long) (pv_time_id & 0x00000000000FFFFFL); // bottom 20 bits only

    // Then for the seconds we right shift 20 bits to get the original number
    // consisting of the top 44 bits
    // pv_time_is         xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyy
    // lv_orig_ts.tv_sec  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    lv_orig_ts.tv_sec = (unsigned long)(pv_time_id >> 20);
    if (gv_verbose)
        printf("srv: long_to_timespec, tv_sec=0x%lx, tv_nsec=0x%lx\n", lv_orig_ts.tv_sec, lv_orig_ts.tv_nsec);

    return lv_orig_ts;
}


int timespec_to_str(char *buf, size_t max_len, struct timespec *ppv_timespec) {

    if (gv_verbose)
        printf("srv: enter timespec_to_str\n");

    unsigned int len;
    char * ptr;
    char tmp_buffer[MAX_DATE_TIME_BUFF_LEN * 2];
    char mon_buff[5];
    char day_buff[3];
    char year_buff[5];
    char time_buff[10];

    // ctime adds a new line character at the end of the string, which we don't want
    strcpy(tmp_buffer, ctime(&ppv_timespec->tv_sec));
    len = (unsigned int)(strlen(tmp_buffer) - 1);
    tmp_buffer[len] = '\0';

    // The current date/time string only has second resolution, and is of
    // this format: 'Fri May  6 03:01:52 2016'.  We need to append the
    // usecs to the time portion (4th token) and create a new string
    // of the format'2016-05-06 03:01:52.123456' 
    ptr = strtok(tmp_buffer, " ");

    // Skip the day of the week and get the month
    ptr = strtok(NULL, " ");
    strcpy(mon_buff, ptr);

    ptr = strtok(NULL, " ");
    if(strlen(ptr) == 1){
      // Pad it with a leading '0'
       day_buff[0] = '0';
       strcpy(&day_buff[1], ptr);
    }
    else {
       strcpy(day_buff, ptr);
    }

    ptr = strtok(NULL, " ");
    strcpy(time_buff, ptr);

    ptr = strtok(NULL, " ");
    strcpy(year_buff, ptr);

    // Convert the name of the month to numerical equivalent
    if(strncmp(mon_buff, "Jan", 3) == 0 ) {
      strcpy(mon_buff, "01");
    }
    else if(strncmp(mon_buff, "Feb", 3) == 0 ) {
      strcpy(mon_buff, "02");
    }
    else if(strncmp(mon_buff, "Mar", 3) == 0 ) {
      strcpy(mon_buff, "03");
    }
    else if(strncmp(mon_buff, "Apr", 3) == 0 ) {
      strcpy(mon_buff, "04");
    }
    else if(strncmp(mon_buff, "May", 3) == 0 ) {
      strcpy(mon_buff, "05");
    }
    else if(strncmp(mon_buff, "Jun", 3) == 0 ) {
      strcpy(mon_buff, "06");
    }
    else if(strncmp(mon_buff, "Jul", 3) == 0 ) {
      strcpy(mon_buff, "07");
    }
    else if(strncmp(mon_buff, "Aug", 3) == 0 ) {
      strcpy(mon_buff, "08");
    }
    else if(strncmp(mon_buff, "Sep", 3) == 0 ) {
      strcpy(mon_buff, "09");
    }
    else if(strncmp(mon_buff, "Oct", 3) == 0 ) {
      strcpy(mon_buff, "10");
    }
    else if(strncmp(mon_buff, "Nov", 3) == 0 ) {
      strcpy(mon_buff, "11");
    }
    else if(strncmp(mon_buff, "Dec", 3) == 0 ) {
      strcpy(mon_buff, "12");
    }
    else  {
      printf("srv: timespec_to_string unrecognized month string %s\n", mon_buff);
      return XZFIL_ERR_FSERR;
    }

    // Now put it all together
    sprintf(tmp_buffer, "%s-%s-%s %s.%ld", year_buff, mon_buff, day_buff, time_buff, ppv_timespec->tv_nsec);
    len = (unsigned int) strlen(tmp_buffer);
    if(len > max_len) {
       return XZFIL_ERR_BADCOUNT;
    }
    strcpy(buf, tmp_buffer);
    return XZFIL_ERR_OK;
}

int str_to_tm_id(char *buf, unsigned long *ppv_tm_id) {

    if (gv_verbose)
        printf("srv: enter str_to_tm_id\n");

    char * ptr;
    char tmp_buffer[MAX_DATE_TIME_BUFF_LEN * 2];
    char date_buff[12];
    char mon_buff[3];
    char day_buff[3];
    char year_buff[5];
    char time_buff[20];
    char hour_buff[3];
    char min_buff[3];
    char sec_usec_buff[12];
    char sec_buff[3];
    char usec_buff[10];
    struct tm lv_tm;
    struct timespec lv_ts;

    // The format of the passed in date string is '2016-05-06 03:01:52.123456'
    // Using a 'space' we separate the input string into date and time components
    strcpy(tmp_buffer, buf);
    ptr = strtok(tmp_buffer, " ");
    strcpy(date_buff, ptr);
    ptr = strtok(NULL, " ");
    strcpy(time_buff, ptr);

    // Tokenize the date using the '-' character
    ptr = strtok(date_buff, "-");
    strcpy(year_buff, ptr);
    ptr = strtok(NULL, "-");
    strcpy(mon_buff, ptr);
    ptr = strtok(NULL, "-");
    strcpy(day_buff, ptr);
  
    // Tokenize the time using the ':' character
    ptr = strtok(time_buff, ":");
    strcpy(hour_buff, ptr);
    ptr = strtok(NULL, ":");
    strcpy(min_buff, ptr);
    ptr = strtok(NULL, ":");
    strcpy(sec_usec_buff, ptr);

    // Tokenize the sec_usec_buff using the '.' character
    ptr = strtok(sec_usec_buff, ".");
    strcpy(sec_buff, ptr);
    ptr = strtok(NULL, ".");
    strcpy(usec_buff, ptr);

    memset(&lv_tm, 0, sizeof(struct tm));
    lv_tm.tm_year = atoi(year_buff) - 1900;
    lv_tm.tm_mon  = atoi(mon_buff) - 1;        // Months are 0 based from January
    lv_tm.tm_mday = atoi(day_buff);
    lv_tm.tm_hour = atoi(hour_buff);
    lv_tm.tm_min  = atoi(min_buff);
    lv_tm.tm_sec  = atoi(sec_buff);

    // Take the tm struct we have created and translate it into the time_t (seconds)
    // portion of a timespec.  We set the nsec portion directly.
    lv_ts.tv_sec = mktime(&lv_tm);
    if (lv_ts.tv_sec == -1){

      // Unsuccessful conversion
      return XZFIL_ERR_FSERR;
    }
    lv_ts.tv_nsec = (atol(usec_buff) * 1000L);

    *ppv_tm_id = ((unsigned long) lv_ts.tv_sec << 20) |
                 ((unsigned long) lv_ts.tv_nsec / 1000);

    if (gv_verbose)
        printf("srv: str_to_tm_id lv_ts =0x%lx,0x%lx converted time 0x%lx\n", lv_ts.tv_sec, lv_ts.tv_nsec, *ppv_tm_id);

    return XZFIL_ERR_OK;
}

//
// Timer callback
//
void timer_callback(int tleid, int toval, short parm1, long parm2) {
    int   ferr;

    if (gv_verbose)
        printf("timer_callback \n");
    reset_time_counter();
    ferr = timer_start_cb(gv_time_refresh_delay, 0, 0, &gv_tleid, &timer_callback);
    assert(ferr == XZFIL_ERR_OK);
}

//
// initialize
//
void do_init(int pv_argc, char **ppp_argv) {
    char *lp_arg;
    int   lv_arg;
    bool  lv_attach;
    int   lv_ferr;
    char *lv_delay_s;

    lv_attach = false;
    for (lv_arg = 1; lv_arg < pv_argc; lv_arg++) {
        lp_arg = ppp_argv[lv_arg];
        if (strcmp(lp_arg, "-attach") == 0)
            lv_attach = true;
        else if (strcmp(lp_arg, "-shook") == 0)
            gv_shook = true;
        else if (strcmp(lp_arg, "-v") == 0)
            gv_verbose = true;
    }
    if (lv_attach)
        lv_ferr = msg_init_attach(&pv_argc, &ppp_argv, false, (char *) "$TSID0");
    else
        lv_ferr = msg_init(&pv_argc, &ppp_argv);
    assert(lv_ferr == XZFIL_ERR_OK);

    gv_time_refresh_delay = 200;  // 2 seconds
    lv_delay_s = getenv("TM_IDTMSRV_REFRESH_DELAY_SECONDS");
    if (lv_delay_s != NULL) {
       gv_time_refresh_delay = 100 * (atoi(lv_delay_s));      
    }
    if (gv_verbose){
        printf("TM_IDTMSRV_REFRESH_DELAY_SECONDS is %s.  Setting gv_time_refresh_delay to %d \n", lv_delay_s, gv_time_refresh_delay);
    }   

    if (gv_shook)
        msg_debug_hook("s", "s");
}

//
// process monitor message.
//
// if shutdown, set done
//
void do_mon_msg(BMS_SRE *pp_sre, bool *pp_done) {
    int        lv_ferr;
    MS_Mon_Msg lv_mon_msg;

    lv_ferr = BMSG_READDATA_(pp_sre->sre_msgId,         // msgid
                             (char *) &lv_mon_msg,      // reqdata
                             (int) sizeof(lv_mon_msg)); // bytecount
    assert(lv_ferr == XZFIL_ERR_OK);
    if (lv_mon_msg.type == MS_MsgType_Shutdown)
        *pp_done = true;
    if (gv_verbose)
        printf("srv: received mon message\n");
    do_reply(pp_sre, NULL, 0, 0);
}

//
// process non-mon message
//
void do_req(BMS_SRE *pp_sre) {
    const char     *lp_req_type;
    short           lv_ec;
    int             lv_ferr;
    int             lv_len;
    GID_Rep         lv_rep;
    GID_Req         lv_req;
    long            lv_req_id;
    struct timeval  lv_tv;
    struct timespec lv_orig_ts;
    unsigned long   lv_converted_tm_id;

    lv_ec = XZFIL_ERR_OK;
    lv_len = 0;
    if (gv_verbose)
        printf("srv: received NON-mon message\n");
    if (pp_sre->sre_reqDataSize < (int) sizeof(lv_req)) {
        if (gv_verbose)
            printf("srv: received short data - sre_reqDataSize=%d, expecting len=%d, setting BADCOUNT\n",
                   pp_sre->sre_reqDataSize, (int) sizeof(lv_req));
        lv_ec = XZFIL_ERR_BADCOUNT;
    } else {
        lv_ferr = BMSG_READDATA_(pp_sre->sre_msgId,      // msgid
                                 (char *) &lv_req,       // reqdata
                                 (int) sizeof(lv_req));  // bytecount
        assert(lv_ferr == XZFIL_ERR_OK);
        if (gv_verbose) {
            switch (lv_req.iv_req_type) {
            case GID_REQ_PING:
                lp_req_type = "ping";
                break;
            case GID_REQ_ID:
                lp_req_type = "id";
                break;
            case GID_REQ_ID_TO_STRING:
                lp_req_type = "id_to_string";
                break;
            case GID_REQ_STRING_TO_ID:
                lp_req_type = "string_to_id";
                break;
            default:
                lp_req_type = "unknown";
                break;
            }
            if (gv_verbose)
                printf("srv: received msg. req-type=%d(%s), tag=%ld, len=%d\n",
                       lv_req.iv_req_type, lp_req_type, lv_req.iv_req_tag, lv_req.iv_req_len);
        }
        switch (lv_req.iv_req_type) {
        case GID_REQ_PING:
            if (lv_req.iv_req_len == (int) sizeof(lv_req.u.iv_ping)) {
                if (gv_verbose)
                    printf("srv: received ping request\n");
                lv_rep.iv_rep_type = GID_REP_PING;
                lv_rep.iv_rep_tag = lv_req.iv_req_tag;
                lv_rep.iv_rep_len = (int) sizeof(lv_rep.u.iv_ping);
                lv_rep.u.iv_ping.iv_com.iv_error = GID_ERR_OK;
                gettimeofday(&lv_tv, NULL);
                lv_rep.u.iv_ping.iv_ts_sec = lv_tv.tv_sec;
                lv_rep.u.iv_ping.iv_ts_us = lv_tv.tv_usec;
            } else {
                if (gv_verbose)
                    printf("srv: received ping, req-len=%d, expecting len=%d, setting BADCOUNT\n",
                           lv_req.iv_req_len, (int) sizeof(lv_req.u.iv_ping));
                lv_ec = XZFIL_ERR_BADCOUNT;
            }
            break;

        case GID_REQ_ID:
            if (lv_req.iv_req_len == (int) sizeof(lv_req.u.iv_id)) {
                if (gv_verbose)
                    printf("srv: received id request\n");
                lv_rep.iv_rep_type = GID_REP_ID;
                lv_rep.iv_rep_tag = lv_req.iv_req_tag;
                lv_rep.iv_rep_len = (int) sizeof(lv_rep.u.iv_id);
                lv_rep.u.iv_id.iv_com.iv_error = GID_ERR_OK;
                lv_rep.u.iv_id.iv_id = __sync_add_and_fetch_8(gp_shm, 1);
            } else {
                if (gv_verbose)
                    printf("srv: received id, req-len=%d, expecting len=%d, setting BADCOUNT\n",
                           lv_req.iv_req_len, (int) sizeof(lv_req.u.iv_id));
                lv_ec = XZFIL_ERR_BADCOUNT;
            }
            break;

        case GID_REQ_ID_TO_STRING:
            if (lv_req.iv_req_len == (int) sizeof(lv_req.u.iv_id_to_string)) {
                lv_rep.iv_rep_type = GID_REP_ID_TO_STRING;
                lv_rep.iv_rep_tag = lv_req.iv_req_tag;
                lv_req_id = lv_req.u.iv_id_to_string.iv_req_id_to_string;
                if (gv_verbose)
                    printf("srv: received id_to_string request for id:Ox%lx\n", lv_req_id);
                lv_orig_ts = long_to_timespec(lv_req_id);
                lv_ferr = timespec_to_str(lv_rep.u.iv_id_to_string.iv_id_to_string, sizeof(lv_rep.u.iv_id_to_string.iv_id_to_string)
                                    , &lv_orig_ts); 
                lv_rep.iv_rep_len = sizeof(lv_rep.u.iv_id_to_string);
                lv_rep.u.iv_id_to_string.iv_com.iv_error = (GID_Err_Type) lv_ferr;
                if (gv_verbose){
                    printf("srv: replying to id_to_string request with err=%d, size %d, and string %s\n",
			 lv_rep.u.iv_id_to_string.iv_com.iv_error, lv_rep.iv_rep_len, lv_rep.u.iv_id_to_string.iv_id_to_string);
                }
            } else {
                if (gv_verbose)
                    printf("srv: received id_to_string, req-len=%d, expecting len=%d, setting BADCOUNT\n",
                           lv_req.iv_req_len, (int) sizeof(lv_req.u.iv_id_to_string));
                lv_ec = XZFIL_ERR_BADCOUNT;
            }
            break;

        case GID_REQ_STRING_TO_ID:
            if (lv_req.iv_req_len == (int) sizeof(lv_req.u.iv_string_to_id)) {
                if (gv_verbose)
                    printf("srv: received string_to_id request\n");
                lv_rep.iv_rep_type = GID_REP_STRING_TO_ID;
                lv_rep.iv_rep_tag = lv_req.iv_req_tag;
                lv_ferr = str_to_tm_id(lv_req.u.iv_string_to_id.iv_string_to_id, &lv_converted_tm_id);
                lv_rep.u.iv_string_to_id.iv_string_to_id = lv_converted_tm_id;
                lv_rep.iv_rep_len = sizeof(lv_rep.u.iv_string_to_id);
                lv_rep.u.iv_string_to_id.iv_com.iv_error = GID_ERR_OK;
            } else {
                if (gv_verbose)
                    printf("srv: received string_to_id, req-len=%d, expecting len=%d, setting BADCOUNT\n",
                           lv_req.iv_req_len, (int) sizeof(lv_req.u.iv_string_to_id));
                lv_ec = XZFIL_ERR_BADCOUNT;
            }
            break;

        default:
            if (gv_verbose)
                printf("srv: received unknown req-type=%d, setting INVALOP\n",
                       lv_req.iv_req_type);
            lv_ec = XZFIL_ERR_INVALOP;
            break;
        }
    }

    if (lv_ec == XZFIL_ERR_OK) {
      if (gv_verbose){
	if(lv_rep.iv_rep_type == GID_REP_ID_TO_STRING){
            printf("srv: reply, rep-type=%d, tag=%ld, id_to_string=%s, len=%d\n",
                   lv_rep.iv_rep_type, lv_rep.iv_rep_tag, lv_rep.u.iv_id_to_string.iv_id_to_string, lv_rep.iv_rep_len);
        }
        else {
            printf("srv: reply, rep-type=%d, tag=%ld, id=%ld, len=%d\n",
                   lv_rep.iv_rep_type, lv_rep.iv_rep_tag, lv_rep.u.iv_id.iv_id, lv_rep.iv_rep_len);
        }
      }
        lv_len = (int) sizeof(lv_rep);
    } else {
        lv_len = 0;
    }

    do_reply(pp_sre, (char *) &lv_rep, lv_len, lv_ec);
}

//
// do reply
//
void do_reply(BMS_SRE *pp_sre, char *pp_reply, int pv_len, short pv_ec) {
    if (gv_verbose)
        printf("srv: reply, len=%d, ec=%d\n", pv_len, pv_ec);
    BMSG_REPLY_(pp_sre->sre_msgId,   // msgid
                NULL,                // replyctrl
                0,                   // replyctrlsize
                pp_reply,            // replydata
                pv_len,              // replydatasize
                pv_ec,               // errorclass
                NULL);               // newphandle
}

//
// setup shared memory.
//
// if shared memory created, then initialize to current time.
//
void do_shm() {
    bool            lv_created;
    int             lv_ferr;
    int             lv_key;
    int             lv_msid;
    struct timespec lv_ts;
    unsigned long   lv_tsl;

    lv_ferr = msg_mon_get_my_segid(&lv_key);
    assert(lv_ferr == XZFIL_ERR_OK);
    if (gv_verbose)
        printf("srv: shm key=%d\n", lv_key);
    lv_msid = shmget(lv_key, sizeof(long), 0640);
    if (lv_msid == -1) {
        lv_created = true;
        if (gv_verbose)
            printf("srv: shmget failed, errno=%d\n", errno);
        lv_msid = shmget(lv_key, sizeof(long), IPC_CREAT | 0640);
        if (lv_msid == -1) {
            if (gv_verbose)
                printf("srv: shmget(IPC_CREAT) failed, errno=%d\n", errno);
            assert(lv_msid != -1);
        } else {
            if (gv_verbose)
                printf("srv: shmget(IPC_CREAT) ok\n");
        }
    } else {
        lv_created = false;
        if (gv_verbose)
            printf("srv: shmget ok\n");
    }
    gp_shm = (char *) shmat(lv_msid, NULL, 0);
    if (gp_shm == NULL) {
        if (gv_verbose)
            printf("srv: shmat failed, errno=%d\n", errno);
        assert(gp_shm != NULL);
    } else {
        gp_shml = (unsigned long *) gp_shm;
        if (gv_verbose)
            printf("srv: shmat ok, shm=%p\n", gp_shm);
        if (lv_created) {
            clock_gettime(CLOCK_REALTIME, &lv_ts);
            // nsec / 1000 => usec
            // 1,000,000 usec requires 20 bits. i.e. 2^20=1,048,576
            // unsigned long is 64 bits, so squeeze secs into leftover 44 bits
            // 100(yrs) * 365(days) * 24(hrs) * 60(min) * 60(sec)=3,153,600,000
            // requires 32 bits
            lv_tsl = ((unsigned long) lv_ts.tv_sec << 20) | ((unsigned long) lv_ts.tv_nsec / 1000);
            *gp_shml = lv_tsl;
            if (gv_verbose)
                printf("srv: initializing shm=0x%lx\n", lv_tsl);
        }
    }
}

//
// server main
//
int main(int pv_argc, char *pa_argv[]) {
    bool     lv_done;
    int      lv_ferr;
    int      lv_lerr;
    BMS_SRE  lv_sre;

    CALL_COMP_DOVERS(idtmsrv, pv_argc, pa_argv);

    do_init(pv_argc, pa_argv);

    lv_ferr = msg_mon_process_startup(true); // system messages
    assert(lv_ferr == XZFIL_ERR_OK);
    msg_mon_enable_mon_messages(true);
    lv_ferr = msg_mon_get_my_process_name(ga_name, sizeof(ga_name));
    assert(lv_ferr == XZFIL_ERR_OK);

    do_shm();
    lv_ferr = timer_start_cb(gv_time_refresh_delay, 0, 0, &gv_tleid, &timer_callback);
    assert(lv_ferr == XZFIL_ERR_OK);

    lv_done = false;
    while (!lv_done) {
        do {
            lv_lerr = XWAIT(LREQ, -1);
            lv_lerr = BMSG_LISTEN_((short *) &lv_sre, // sre
                                   0,                 // listenopts
                                   0);                // listenertag
        } while (lv_lerr == XSRETYPE_NOWORK);
        if (lv_sre.sre_flags & XSRE_MON) {
            do_mon_msg(&lv_sre, &lv_done);
        } else {
            do_req(&lv_sre);
        }
    }

    if (gv_verbose)
        printf("server %s shutting down\n", ga_name);
    lv_ferr = msg_mon_process_shutdown();
    assert(lv_ferr == XZFIL_ERR_OK);

    return 0;
}
