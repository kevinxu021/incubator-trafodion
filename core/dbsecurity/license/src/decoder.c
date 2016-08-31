/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/des.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


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

//define the enum of package installed
#define PACKAGE_ENT  1
#define PACKAGE_ADV  2

#define PACKAGE_ENT_TEXT  "ENT"
#define PACKAGE_ADV_TEXT  "ADV"

//define the enum of installed type
#define TYPE_DEMO     1
#define TYPE_POC      2
#define TYPE_PRODUCT  3
#define TYPE_INTERNAL 4

#define TYPE_DEMO_TEXT "DEMO"
#define TYPE_POC_TEXT "POC"
#define TYPE_PRODUCT_TEXT "PRODUCT"
#define TYPE_INTERNAL_TEXT "INTERNAL"

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
        -s <encrypted license string>\n\
        -f <encrypted file name>\n\
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
    int vopt=0;
    int fopt=0;
    char encstr[ENC_LEN+1];
    char encfile[1024];
    int nodenumber = 0;
    int exday = 0;
    int fd = 0;
    short vernum = 0;
    int packageinstalled = 0;
    int installtype=0;
    memset(encstr, 0, sizeof(encstr));
    memset(encfile, 0, sizeof(encfile));
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

    while((ch=getopt(argc,argv,"cnetpvf:s:"))!=-1)
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
            case 'v':
                vopt=1;
                argnum++;
                break;
            case 'f':
                strcpy(encfile,optarg);
                fd=open(encfile,O_RDONLY);
                if(fd == -1) {perror("File open error"); exit(1); }
                read(fd,encstr, ENC_LEN);
                close(fd);
                fopt=1;
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
    
    //decodedbuf contains the DES encryted string
    //input string is in ASCII format, each encrypted char is represented as 2 bytes in HEX value
    //strpack transform the HEX into original value
    char decodedbuf[inputlen/2 + 1];
    memset(decodedbuf, 0 , sizeof(decodedbuf) );
    strpack(encstr, sizeof(encstr), (char*) decodedbuf);

    size_t len =(sizeof(decodedbuf)+7)/8 * 8;
    unsigned char *output = new unsigned char[len+1];
    DES_cblock ivec;
    memset((char*)&ivec, 0, sizeof(ivec));

    //decipher
    DES_ncbc_encrypt((const unsigned char *)decodedbuf, output, len, &key_schedule, &ivec, 0);

    //parse the encstr
    char display[64]; memset(display, 0, sizeof(display)); 
    if(copt == 1)  //print customer
    {
        strncpy(display, (char*)output + VERSION_LEN,  CUSTOMER_LEN);
        printf("%s\n",  display);
    }
    else if(vopt == 1)
    {
        memcpy(&vernum, (char*)output ,  sizeof(short));
        printf("%d\n", vernum); 
    }
    else if(eopt == 1)  //print expire date
    {
        memcpy(&exday, (char*)output + VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN,  sizeof(int));
        printf("%d\n", exday); 
    }
    else if(nopt == 1)  //print node number
    {
        memcpy(&nodenumber, (char*)output + VERSION_LEN + CUSTOMER_LEN ,  sizeof(int));
        printf("%d\n",  nodenumber);
    }
    else if(topt == 1)  //print type
    {
        memcpy(&installtype, (char*)output + VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN + EXPIRE_LEN + PACKAGE_INSTALLED,  sizeof(int));
        switch(installtype) 
        {
          case TYPE_DEMO:
            printf("%s\n",TYPE_DEMO_TEXT);
            break;
          case TYPE_POC:
            printf("%s\n",TYPE_POC_TEXT);
            break;
          case TYPE_PRODUCT:
            printf("%s\n", TYPE_PRODUCT_TEXT);
            break;
          case TYPE_INTERNAL:
            printf("%s\n", TYPE_INTERNAL_TEXT);
            break;
          default:
            printf("UNKNOWN : %d\n", installtype);
        }
    }
    else if(popt == 1)  //print package
    {
        memcpy(&packageinstalled, (char*)output + VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN + EXPIRE_LEN,  sizeof(int));
        switch(packageinstalled)
        {
          case PACKAGE_ENT:
            printf("%s\n",PACKAGE_ENT_TEXT);
            break;
          case PACKAGE_ADV:
            printf("%s\n",PACKAGE_ADV_TEXT);
            break;
          default:
            printf("UNKNOWN\n");
        } 
    }

    return ret;
}
