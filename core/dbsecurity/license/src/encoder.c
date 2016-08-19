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

void printHelp()
{
    printf("\
encoder –v [version]\n\
        -c [customer name]\n\
        -n [node number]\n\
        -e [expire date]\n\
        -p [package installed]\n\
        -t [install type]\n"\
        );
}

int main(int argc, char *argv[])
{
    int ret = 0;
    int ch = 0;
    int i = 0;
    int v = 0;
    int n = 0;
    int argnum=0;
    int da = 0;
    int len = 0;
    
    char version[VERSION_LEN+1];
    char customer[CUSTOMER_LEN+1];
    char nodenumber[NODENUM_LEN+1];
    char expiredate[EXPIRE_LEN+1];
    char packageInstalled[PACKAGE_INSTALLED+1];
    char installType[INSTALL_TYPE+1];
    
    /* initialize string buffer */
    memset(version,0,VERSION_LEN+1);
    memset(customer,0,CUSTOMER_LEN+1);
    memset(nodenumber,0,NODENUM_LEN+1);
    memset(expiredate,0,EXPIRE_LEN+1);
    memset(packageInstalled,0,PACKAGE_INSTALLED+1);
    memset(installType,0,INSTALL_TYPE+1);

    while((ch=getopt(argc,argv,"v:c:n:e:p:t:"))!=-1)
    {
        switch(ch)
        {
            case 'v':
                v = atoi(optarg);
                if ( v < 1 || v > 9 ) // 1 char, 9 version should be enough ...
                {
                    printf("Version %s is invalid\n", optarg);
                    exit(1);
                }
                sprintf(version,"%2d",v);
                argnum++;
                break;
            case 'c':
                sprintf(customer,"%10s",optarg );
                argnum++;
                break;
            case 'n':
                n = atoi(optarg);
                if ( n < 1 || n > 9999 )
                {
                    printf("node number %s is invalid\n", optarg);
                    exit(1);
                }               
                sprintf(nodenumber,"%4d",n);
                argnum++;
                break;
            case 'e':
                //set to 9999 for now
                da = atoi(optarg);
                sprintf(expiredate,"%4d",da);
                argnum++;
                break;
            case 'p':
                //set to 9999 for now
                len = PACKAGE_INSTALLED ;
                if(len > strlen(optarg) )
                   len = strlen(optarg);
                strncpy(packageInstalled,optarg,len);
                argnum++;
                break;
            case 't':
                //set to 9999 for now
                len = INSTALL_TYPE;
                if(len > strlen(optarg) )
                   len = strlen(optarg); 
                strncpy(installType,optarg,len);
                argnum++;
                break;
            default:
                printHelp();
                exit(1);
        }
    }
    if(argnum != 6)
    {
        printHelp();
        exit(1);
    }
        
    char output[MAX_ENC_STR_LEN];
    memset(output,0,MAX_ENC_STR_LEN);
    
    strcat(output,version);
    strcat(output,customer);
    strcat(output,nodenumber);
    strcat(output,expiredate);
    strcat(output,packageInstalled);
    strcat(output,installType);
        
    //encrpt
    DES_cblock key[1];
    DES_key_schedule key_schedule;

    DES_string_to_key("nonstop2016",key);

    if (DES_set_key_checked(key, &key_schedule) != 0)
      exit(1);
    
    size_t lenenc = (strlen(output) +7)/8 * 8;
    unsigned char *outputenc = (unsigned char*) malloc(lenenc+1);
    memset(outputenc, 0 , sizeof(outputenc) );

    DES_cblock ivec;

    memset((char*)&ivec, 0, sizeof(ivec));
    DES_ncbc_encrypt((const unsigned char *)output, outputenc, lenenc, &key_schedule, &ivec, 1);

    //convert each character into HEX and output 
    for( i=0 ; i < lenenc; i++)
    {
        printf("%.2x",outputenc[i]);
    }
 
    return ret;
}
