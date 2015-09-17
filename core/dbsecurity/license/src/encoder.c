/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_STR_LEN  512
//ASCII is smaller than 256, so 2 bytes to show in HEX 
#define MAX_ENC_STR_LEN  MAX_STR_LEN*2

#define VERSION_LEN     1
#define CUSTOMER_LEN    7
#define NODENUM_LEN     4
#define EXPIRE_LEN      4

void printHelp()
{
    printf("\
encoder –v [version]\n\
        -c [customer name]\n\
        -n [node number]\n\
        -e [expire date]\n");
}

int main(int argc, char *argv[])
{
    int ret = 0;
    int ch = 0;
    int i = 0;
    int v = 0;
    int n = 0;
    int argnum=0;
    
    char version[VERSION_LEN+1];
    char customer[CUSTOMER_LEN+1];
    char nodenumber[NODENUM_LEN+1];
    char expiredate[EXPIRE_LEN+1];
    
    /* initialize string buffer */
    memset(version,0,VERSION_LEN+1);
    memset(customer,0,CUSTOMER_LEN+1);
    memset(nodenumber,0,NODENUM_LEN+1);
    memset(expiredate,0,EXPIRE_LEN+1);

    while((ch=getopt(argc,argv,"v:c:n:e:"))!=-1)
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
                sprintf(version,"%1d",v);
                argnum++;
                break;
            case 'c':
                sprintf(customer,"%7s",optarg );
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
                strcpy(expiredate,"9999");
                argnum++;
                break;
            default:
                printHelp();
                exit(1);
        }
    }
    if(argnum != 4)
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
        
    //encrpt
    //convert each character into HEX and output 
    for( i=0 ; i < strlen(output); i++)
    {
        printf("%2x",output[i]);
    }

    return ret;
}
