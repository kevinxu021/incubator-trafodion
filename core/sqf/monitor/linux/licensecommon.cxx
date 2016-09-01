#include "licensecommon.h"

static unsigned char asc2hex[] = {
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
     0, 10, 11, 12, 13, 14, 15,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0, 10, 11, 12, 13, 14, 15,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0
};

CLicenseCommon::CLicenseCommon()
{    
    secsToStartWarning_ = LICENSE_SEVEN_DAYS;
    char *daysToStartWarning = getenv("SQ_DAYS_START_WARNING");
    if (daysToStartWarning)
    {
       secsToStartWarning_ = atoi (daysToStartWarning);
       secsToStartWarning_ = secsToStartWarning_*LICENSE_ONE_DAY; // Convert to seconds       
    }

    action_ = HC_KEEP_WARNING;
    char *actionToTake = getenv("SQ_LICENSE_ACTION");
    if (actionToTake)
    {
       action_ = (HealthCheckLicenseAction)atoi (actionToTake);
    }
    licenseReady_ = false;
    parseLicense();
}

CLicenseCommon::~CLicenseCommon()
{
  
}
   
void CLicenseCommon::parseLicense()
{
    FILE *pFile;
    int   bytesRead = 0;
    char  myLicense[LICENSE_NUM_BYTES*3];  // some extra space just in case.  Update on next checkin [TRK]
    char  buffer[MAX_FILE_NAME+MAX_PROCESS_PATH];
    char *tmpDir = NULL;

    memset(myLicense,0,LICENSE_NUM_BYTES*3);  // some extra space just in case.  Update on next checkin [TRK]
   
    char *licenseFile = getenv("SQ_MON_LICENSE_FILE");
    if (!licenseFile)
    {
        // let's check to see if it is in sql/scripts since this might be a dev cluster
        tmpDir = getenv( "MY_SQROOT" );
        if (!tmpDir)
        {
           return;
        }
        
        sprintf(buffer, "%s/sql/scripts/licenseENC", tmpDir);
    }
    else
    {
        sprintf (buffer, "%s", licenseFile);
    }

    pFile = fopen( buffer, "r" );
    if ( pFile )
    {
        bytesRead = fread (myLicense,sizeof(char), sizeof(myLicense),pFile);
        fclose(pFile);
    }
    
    if ( bytesRead  != LICENSE_NUM_BYTES_ENC)
    {
       return;
    }
       
    // The below code was stolen from the license decoder.  Should be common place?
    DES_cblock key[1];
    DES_key_schedule key_schedule;
 
       
    DES_string_to_key("nonstop2016",key);

    if (DES_set_key_checked(key, &key_schedule) != 0)
    {
         return;
    }

    int inputlen = strlen(myLicense); 
    
    //decodedbuf contains the DES encryted string
    //input string is in ASCII format, each encrypted char is represented as 2 bytes in HEX value
    //strpack transform the HEX into original value
    char decodedbuf[inputlen/2 + 1];
    memset(decodedbuf, 0 , sizeof(decodedbuf) );
    strpack(myLicense, sizeof(myLicense), (char*) decodedbuf);

    size_t len =(sizeof(decodedbuf)+7)/8 * 8;
    unsigned char *output = new unsigned char[len+1];
    DES_cblock ivec;
    memset((char*)&ivec, 0, sizeof(ivec));

    //decipher
    DES_ncbc_encrypt((const unsigned char *)decodedbuf, output, len, &key_schedule, &ivec, 0);

    memcpy ((void*)license_, output,LICENSE_NUM_BYTES );
    memcpy ((void*)&version_, &(license_[LICENSE_VERSION_OFFSET]), sizeof(short));
    memcpy ((void*)&numNodes_, &(license_[LICENSE_NODES_OFFSET]), sizeof(int));
    memcpy ((void*)&expireDays_, &(license_[LICENSE_EXPIRE_OFFSET]), sizeof(int));
    strncpy (customerName_, (char *)license_ + LICENSE_NAME_OFFSET, 10);
    memcpy ((void*)&package_, &(license_[LICENSE_PACKAGE_OFFSET]), 4);
    memcpy ((void*)&type_, &(license_[LICENSE_TYPE_OFFSET]), 4);   
    memcpy ((void*)reserved_, &(license_[LICENSE_RESERVED_OFFSET]), 4);

   licenseReady_ = true;
}
   
bool CLicenseCommon::isInternal()
{
     if (type_ == TYPE_INTERNAL)
     {
         return true;
     }
     return false;
}

char * CLicenseCommon::strpack(char *src, size_t len, char *dst)
{
    unsigned char *from, *to, *end;
 
    from = (unsigned char *)src;
    to = (unsigned char *)dst;
 
    for (end = to + len / 2; to < end; from += 2, to++)
    {
        *to  = (asc2hex[*from] << 4) | asc2hex[*(from + 1)];
    }
    
    return dst;
}