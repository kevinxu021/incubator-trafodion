/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/des.h>
#include <ctype.h>

#define MAX_ENC_STR_LEN  512

#define VERSION_LEN        2
#define CUSTOMER_LEN       10
#define NODENUM_LEN        4
#define EXPIRE_LEN         4
#define PACKAGE_INSTALLED  4
#define INSTALL_TYPE       4
#define RESERVED_FIELD     4

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
    short v = 0;
    int n = 0;
    int argnum=0;
    int da = 0;
    int len = 0;
    int package=0, type=0;
    
    char version[VERSION_LEN+1];
    char customer[CUSTOMER_LEN+1];
    int nodenumber;
    int expiredate;
    char packageInstalled[PACKAGE_INSTALLED+1];
    char installType[INSTALL_TYPE+1];
    char typeUpper[16]; 
    char pkgUpper[16]; 
    
    /* initialize string buffer */
    memset(customer,0,CUSTOMER_LEN+1);
    memset(packageInstalled,0,PACKAGE_INSTALLED+1);
    memset(installType,0,INSTALL_TYPE+1);

    while((ch=getopt(argc,argv,"v:c:n:e:p:t:"))!=-1)
    {
        switch(ch)
        {
            case 'v':
                v = atoi(optarg);
                if ( v < 1 || v > 128 )  
                {
                    printf("Version %s is invalid\n", optarg);
                    exit(1);
                }
                argnum++;
                break;
            case 'c':
                sprintf(customer,"%10s",optarg );
                argnum++;
                break;
            case 'n':
                nodenumber = atoi(optarg);
                if ( nodenumber < 1 || nodenumber > 9999 )
                {
                    printf("node number %s is invalid\n", optarg);
                    exit(1);
                }               
                argnum++;
                break;
            case 'e':
                // 32-bit integer, # of days after Jan 1, 1970, 4 bytes
                expiredate= atoi(optarg);
                argnum++;
                break;
            case 'p':
                package = 0;
                memset(pkgUpper, 0, sizeof(pkgUpper)); 
                if (strlen(optarg) > 16) {
                  printf("Invalid package\n");
                  exit(1);
                }
                for(i = 0; i < strlen(optarg); i++)
                  pkgUpper[i]=toupper(optarg[i]);
                if(strcmp(pkgUpper,PACKAGE_ADV_TEXT) == 0 ) 
                  package=PACKAGE_ADV;
                else if(strcmp(pkgUpper,PACKAGE_ENT_TEXT) == 0)
                  package=PACKAGE_ENT;
                memcpy(packageInstalled,(void*)&package,sizeof(int));
                argnum++;
                break;
            case 't':
                type= 0;
                memset(typeUpper, 0, sizeof(typeUpper)); 
                if (strlen(optarg) > 16) {
                  printf("Invalid type \n");
                  exit(1);
                }
                for(i = 0; i < strlen(optarg); i++)
                  typeUpper[i]=toupper(optarg[i]);
                if(strcmp(typeUpper,TYPE_DEMO_TEXT) == 0 ) 
                  type=TYPE_DEMO;
                else if(strcmp(typeUpper,TYPE_POC_TEXT) == 0)
                  type=TYPE_POC;
                else if(strcmp(typeUpper, TYPE_PRODUCT_TEXT) == 0)
                  type=TYPE_PRODUCT;
                else if(strcmp(typeUpper, TYPE_INTERNAL_TEXT) == 0)
                  type=TYPE_INTERNAL;
                else
                  type=0;
                memcpy(installType,(void*)&type,sizeof(int));
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
   
    memcpy(output , &v, sizeof(short));
    memcpy(output + VERSION_LEN, customer, CUSTOMER_LEN );
    memcpy(output + VERSION_LEN + CUSTOMER_LEN , &nodenumber, sizeof(int));
    memcpy(output + VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN , &expiredate , sizeof(int));
    memcpy(output + VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN + EXPIRE_LEN , packageInstalled , sizeof(int));
    memcpy(output + VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN + EXPIRE_LEN + PACKAGE_INSTALLED , installType, sizeof(int));
        
    //encrpt
    DES_cblock key[1];
    DES_key_schedule key_schedule;

    DES_string_to_key("nonstop2016",key);

    if (DES_set_key_checked(key, &key_schedule) != 0)
      exit(1);

    //DES requires the output buffer len to be  ( inputLen + 7 ) /8 * 8  , i.e. output is longer than input
    size_t lenenc = (VERSION_LEN + CUSTOMER_LEN + NODENUM_LEN + EXPIRE_LEN + PACKAGE_INSTALLED + INSTALL_TYPE + RESERVED_FIELD +7)/8 * 8;
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
