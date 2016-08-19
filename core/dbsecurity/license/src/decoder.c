/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/des.h>

#define MAX_STR_LEN  512
//ASCII is smaller than 256, so 2 bytes to show in HEX 
#define MAX_ENC_STR_LEN  MAX_STR_LEN*2

#define VERSION_LEN        2
#define CUSTOMER_LEN       10 
#define NODENUM_LEN        4
#define EXPIRE_LEN         4
#define PACKAGE_INSTALLED  4
#define INSTALL_TYPE       4
#define RESERVED_FIELD     4
#define ENC_LEN  (VERSION_LEN+CUSTOMER_LEN+NODENUM_LEN+EXPIRE_LEN+EXPIRE_LEN + PACKAGE_INSTALLED+INSTALL_TYPE+RESERVED_FIELD)*2

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
 
char * strpack(char *src, size_t len, char *dst)
{
    unsigned char *from, *to, *end;
 
    from = (unsigned char *)src;
    to = (unsigned char *)dst;
 
    for (end = to + len / 2; to < end; from += 2, to++)
        *to  = (asc2hex[*from] << 4) | asc2hex[*(from + 1)];
 
    return dst;
}

void printHelp()
{
    printf("\
decoder -c\n\
        -n\n\
        -s [encrypted license string]\n\
        -p\n\
        -t\n\
        -e\n");
}

int main(int argc, char *argv[])
{
    int ret = 0;
    int ch = 0;
    int i = 0;
    int v = 0;
    int n = 0;
    int argnum=0;
    int copt=0;
    int sopt=0;
    int nopt=0;
    int eopt=0;
    int topt=0;
    int popt=0;
    char encstr[ENC_LEN+1];
    memset(encstr, 0, sizeof(encstr));
#if 0
    char version[VERSION_LEN+1];
    char customer[CUSTOMER_LEN+1];
    char nodenumber[NODENUM_LEN+1];
    char expiredate[EXPIRE_LEN+1];
    char packageinstalled[PACKAGE_INSTALLED+1];
    char installtype[INSTALL_TYPE+1];
    char reserved[RESERVED_FIELD+1];
    
    /* initialize string buffer */
    memset(version,0,VERSION_LEN+1);
    memset(customer,0,CUSTOMER_LEN+1);
    memset(nodenumber,0,NODENUM_LEN+1);
    memset(expiredate,0,EXPIRE_LEN+1);
    memset(packageinstalled, 0 , PACKAGE_INSTALLED+1);
    memset(installtype, 0, INSTALL_TYPE+1);
    memset(reserved, 0, RESERVED_FIELD+1);
#endif 

    while((ch=getopt(argc,argv,"cnetps:"))!=-1)
    {
        switch(ch)
        {
            case 's':
                strncpy(encstr,optarg,ENC_LEN);
                sopt=1;
                argnum++;
                break;
            case 'c':
                copt=1;
                argnum++;
                break;
            case 'n':
                nopt=1;
                argnum++;
                break;
            case 't':
                topt=1;
                argnum++;
                break;
            case 'p':
                popt=1;
                argnum++;
                break;
            case 'e':
                eopt=1;
                argnum++;
                break;
            default:
                printHelp();
                exit(1);
        }
    }
    if(argnum < 1 || argnum > 2)
    {
        printHelp();
        exit(1);
    }

    DES_cblock key[1];
    DES_key_schedule key_schedule;
   
    DES_string_to_key("nonstop2016",key);

    if (DES_set_key_checked(key, &key_schedule) != 0)
      exit(1);

    int inputlen = strlen(encstr); 

    char decodedbuf[inputlen/2 + 1];
    memset(decodedbuf, 0 , sizeof(decodedbuf) );
    strpack(encstr, sizeof(encstr), (char*) decodedbuf);

    size_t len =(sizeof(decodedbuf)+7)/8 * 8;
    unsigned char *output = new unsigned char[len+1];
    DES_cblock ivec;
    memset((char*)&ivec, 0, sizeof(ivec));

    DES_ncbc_encrypt((const unsigned char *)decodedbuf, output, len, &key_schedule, &ivec, 0);

    //parse the encstr
    char display[64]; memset(display, 0, sizeof(display)); 
    if(copt == 1)  //print customer
    {
        strncpy(display, (char*)output+ VERSION_LEN,  CUSTOMER_LEN);
    }
    else if(eopt == 1)  //print expire date
    {
        strncpy(display, (char*)output+ VERSION_LEN+CUSTOMER_LEN+NODENUM_LEN,  EXPIRE_LEN);
    }
    else if(nopt == 1)  //print node number
    {
        strncpy(display, (char*)output + VERSION_LEN+CUSTOMER_LEN,  NODENUM_LEN);
    }
    else if(topt == 1)  //print node number
    {
        strncpy(display, (char*)output + VERSION_LEN+CUSTOMER_LEN+NODENUM_LEN+EXPIRE_LEN+PACKAGE_INSTALLED,  INSTALL_TYPE);
    }
    else if(popt == 1)  //print node number
    {
        strncpy(display, (char*)output + VERSION_LEN+CUSTOMER_LEN+NODENUM_LEN+EXPIRE_LEN,  PACKAGE_INSTALLED);
    }
    printf("%s",  display);

    return ret;
}
