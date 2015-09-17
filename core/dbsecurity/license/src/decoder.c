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
#define ENC_LEN  (VERSION_LEN+CUSTOMER_LEN+NODENUM_LEN+EXPIRE_LEN+EXPIRE_LEN)*2

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
encoder -c\n\
        -n\n\
        -s [encrypted license string]\n\
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
    
    char version[VERSION_LEN+1];
    char customer[CUSTOMER_LEN+1];
    char nodenumber[NODENUM_LEN+1];
    char expiredate[EXPIRE_LEN+1];
    char encstr[ENC_LEN+1];
    
    /* initialize string buffer */
    memset(version,0,VERSION_LEN+1);
    memset(customer,0,CUSTOMER_LEN+1);
    memset(nodenumber,0,NODENUM_LEN+1);
    memset(expiredate,0,EXPIRE_LEN+1);
    

    while((ch=getopt(argc,argv,"cnes:"))!=-1)
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
        
    //parse the encstr
    char oneChar[3]; //HEX char
    if(copt == 1)  //print customer
    {
        strpack(encstr+2, CUSTOMER_LEN * 2, customer);
        printf("%s",customer);
    }
    if(eopt == 1)  //print expire date
    {
        strpack(encstr + (CUSTOMER_LEN + 2 + 4)*2, EXPIRE_LEN * 2, expiredate);
        printf("%s",expiredate);
    }
    if(nopt == 1)  //print node number
    {
        strpack(encstr + (CUSTOMER_LEN + 1  )*2, NODENUM_LEN * 2, nodenumber);
        printf("%s",nodenumber);
    }

    return ret;
}
