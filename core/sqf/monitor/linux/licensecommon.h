#ifndef LICENSECOMMON_H
#define LICENSECOMMON_H

using namespace std;

#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <sys/epoll.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <openssl/des.h>
#include <unistd.h>

#include "msgdef.h"
#include "internal.h"

typedef enum
{
  HC_STOP_INSTANCE=1,
  HC_KEEP_WARNING
} HealthCheckLicenseAction;

class CLicenseCommon {
  public:
    CLicenseCommon();
    ~CLicenseCommon();
  
    inline HealthCheckLicenseAction getAction(){return action_;}
    inline char *getLicense() {return license_;}
    inline bool  getLicenseReady() {return licenseReady_;}
    inline int   getSecsToStartWarning(){return secsToStartWarning_;}
    
    inline short getVersion() {return version_;}
    inline char *getCustomerName() {return customerName_;}
    inline int   getNumNodes() {return numNodes_;}
    inline int   getExpireDays() {return expireDays_;}
    inline int   getPackage() {return package_;}
    inline int   getType() {return type_;}
    inline char *getReserved() {return reserved_;}
    bool         isInternal();
    
  private:
   HealthCheckLicenseAction action_;   // what are we supposed to do when license expires?
   char  license_[LICENSE_NUM_BYTES];
   bool  licenseReady_;                 // Did we read in a successful license?
   int   secsToStartWarning_;           // how many seconds before and after we should warn about license expiration
   

   char  customerName_[LICENSE_NAME_SIZE+1];
   int   expireDays_;
   int   numNodes_;
   int   package_;
   char  reserved_[LICENSE_RESERVED_SIZE+1];
   int   type_;
   short version_;
   
   void  parseLicense();
   char *strpack(char *src, size_t len, char *dst);
};

#endif