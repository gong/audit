#include<stdio.h>
#include<cassandra.h>
#include<gmp.h>
#include<miracl/miracl.h>
#include<time.h>
#include<string.h>
#include<stdlib.h>
#include<stdbool.h>
void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

void bytetoHexstr( const char *sSrc,char** sDest,int nSrcLen )
{
     int  i;
     char szTmp[3];
     //char* sDest=(char*)malloc(sizeof(char)*65);
     for( i = 0; i < nSrcLen; i++ )
     {
         sprintf( szTmp, "%02X", (unsigned char) sSrc[i] );
          //memcpy( &sDest[i * 2], szTmp, 2 );
         strcpy(&(*sDest)[i*2],szTmp);
     }
}

CassError select_from_userkey_user(CassSession* session,char username[100][50],char filename[100][50],int* usernum) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT username,filename FROM myproject.userkey";

  statement = cass_statement_new(query,0);
  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);
    int i=0;
    while(cass_iterator_next(iterator)) {
      const CassValue* filenamevalue= NULL;
      const CassValue* usernamevalue=NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      size_t usernamesize,filenamesize;
      usernamevalue=cass_row_get_column(row, 0);
      filenamevalue=cass_row_get_column(row,1);
      const char* usernametemp;
      cass_value_get_string(usernamevalue,&usernametemp,&usernamesize);
      memcpy(username[i],usernametemp,usernamesize);

      const char* filenametemp;
      cass_value_get_string(filenamevalue,&filenametemp,&filenamesize);
      memcpy(filename[i],filenametemp,filenamesize);
      i++;
     
    }
   *usernum=i;
    cass_result_free(result);
    cass_iterator_free(iterator);
  }
  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}
CassError select_from_usertag(CassSession* session,char* username,char* filename,int blocknum,char** tag)
{
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query="SELECT tag FROM myproject.usertag WHERE username= ? and filename= ? and blocknum= ?";
   statement = cass_statement_new(query,3);
   
   cass_statement_bind_string(statement,0,username);
   cass_statement_bind_string(statement,1,filename);
   cass_statement_bind_int32(statement,2,blocknum);
   
    future = cass_session_execute(session,statement);
    cass_future_wait(future);

    rc=cass_future_error_code(future);
    if(rc!=CASS_OK)
     print_error(future);
    else{
      const CassResult* result= cass_future_get_result(future);
      CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassValue* value= NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      size_t strtag_length;
      const char* strtag=NULL;
      value=cass_row_get_column(row, 0);
      cass_value_get_string(value,&strtag,&strtag_length);
      sprintf(*tag,"%.*s",(int)strtag_length,strtag);
      //memcpy(*tag, strtag,strtag_length);
      //printf("tag=%s\n",*tag);
      }
     cass_result_free(result);
     cass_iterator_free(iterator);
    }
  cass_statement_free(statement);
  cass_future_free(future);
  return rc;
}
CassError select_from_userkey(CassSession* session,char* username,char* filename,char** p,char** q,char** sk,char* g[]) {
  const char* ptemp=NULL;
  const char* sktemp=NULL;
  const char* qtemp=NULL;
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT p,q,sk,g FROM myproject.userkey WHERE  username= ? and filename= ?";

  statement = cass_statement_new(query, 2);

  cass_statement_bind_string(statement, 0, username);
  cass_statement_bind_string(statement,1,filename);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassValue* pvalue= NULL;
      const CassValue* qvalue=NULL;
      const CassValue* skvalue=NULL;
      const CassValue* gvalue=NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      CassIterator* g_iterator = NULL;

      size_t psize,sksize,qsize;
      pvalue=cass_row_get_column(row, 0);
      qvalue=cass_row_get_column(row,1);
      skvalue=cass_row_get_column(row,2);
      gvalue=cass_row_get_column(row,3);

      cass_value_get_string(pvalue,&ptemp,&psize);
      *p=(char*)malloc(psize+1);
     // memcpy(*p,ptemp,psize);
     sprintf(*p,"%.*s",(int)psize,ptemp);

      cass_value_get_string(qvalue,&qtemp,&qsize);
      *q=(char*)malloc(qsize+1);
      //memcpy(*q,qtemp,qsize);
      sprintf(*q,"%.*s",(int)qsize,qtemp);
     // printf("这里q=%s\n",*q);
      cass_value_get_string(skvalue,&sktemp,&sksize);
      *sk=(char*)malloc(sksize+1);
      //memcpy(*sk,sktemp,sksize);
      sprintf(*sk,"%.*s",(int)sksize,sktemp);

      g_iterator=cass_iterator_from_collection(gvalue);
      int i=0;
      while (cass_iterator_next(g_iterator)) {
        const char* strg=NULL;
        size_t strg_length;
        cass_value_get_string(cass_iterator_get_value(g_iterator), &strg, &strg_length);
        //printf("g: %.*s\n", (int)strg_length, strg);
       // memcpy(g[i++],strg,strg_length);
         g[i]=(char*)malloc(strg_length+1);
         sprintf(g[i++],"%.*s",(int)strg_length,strg);
        }
     cass_iterator_free(g_iterator);
    }
    cass_result_free(result);
    cass_iterator_free(iterator);
  }
  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError  select_from_userdata_n(CassSession* session,char* username,char* filename,long* n) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT count(*) FROM myproject.userdata WHERE  username= ? and filename= ?";

  //cass_int64_t count;

  statement = cass_statement_new(query, 2);

  cass_statement_bind_string(statement, 0, username);
  cass_statement_bind_string(statement,1,filename);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassValue* nvalue=NULL;
      const CassRow* row = cass_iterator_get_row(iterator);

      nvalue=cass_row_get_column(row,0);

      cass_value_get_int64(nvalue,n);
      }
   // printf("n=%ld\n",*n);
    cass_result_free(result);
    cass_iterator_free(iterator);
    }
  cass_future_free(future);
  cass_statement_free(statement);
  return rc;
}

CassError select_from_userdata(CassSession* session,char* username,char* filename,int blocknum,char* file[]) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT file FROM myproject.userdata WHERE  username= ? and filename= ? and blocknum= ?";

  statement = cass_statement_new(query,3);

  cass_statement_bind_string(statement, 0, username);
  cass_statement_bind_string(statement,1,filename);
  cass_statement_bind_int32(statement,2,blocknum);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassValue* filevalue=NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      CassIterator* file_iterator = NULL;

      filevalue=cass_row_get_column(row,0);
      file_iterator=cass_iterator_from_collection(filevalue);
      int i=0;
      while (cass_iterator_next(file_iterator)) {
        const char* strfile=NULL;
        size_t strfile_length;
        cass_value_get_string(cass_iterator_get_value(file_iterator), &strfile, &strfile_length);
       // if(34<strlen(strfile))
           // printf("越界写 %d",(int)strlen(strfile));
        memcpy(file[i++],strfile,strfile_length);
        //printf("file=%s\n%d\n",file[i-1],(int)strfile_length);
       // printf("i=%d\n",i-1);
      }
      cass_iterator_free(file_iterator);
    }
  
    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}


void challenge(mpz_t k1,mpz_t k2,char* q){
  mpz_t mpz_tq;
  //mpz_init(mpz_tq);
  mpz_init_set_str(mpz_tq,q,10);
  gmp_randstate_t rstate;
  gmp_randinit_default(rstate);
  gmp_randseed_ui(rstate,time(NULL));
  mpz_urandomm(k1,rstate,mpz_tq);
  mpz_urandomm(k2,rstate,mpz_tq);
  mpz_clear(mpz_tq);
  gmp_randclear(rstate);
}
 
void PRF(mpz_t k2,long i,mpz_t a,char* q){
  gmp_randstate_t rstate;
  mpz_t mpz_tq;
  mpz_init(mpz_tq);
  mpz_set_str(mpz_tq,q,10);
  gmp_randinit_default(rstate);
  mpz_add_ui(k2,k2,(unsigned long)i);
  gmp_randseed(rstate,k2);
  mpz_urandomm(a,rstate,mpz_tq);
  gmp_randclear(rstate);
  mpz_clear(mpz_tq);
}

long PRP(mpz_t k1,long i,int n){
  gmp_randstate_t rstate;
  long num;
  mpz_t mpz_tn;
  mpz_t rand;
  mpz_init(rand);
  mpz_init(mpz_tn);
  mpz_set_ui(mpz_tn,(unsigned long)n);
  gmp_randinit_default(rstate);
  mpz_add_ui(k1,k1,(unsigned long)i);
  gmp_randseed(rstate,k1);
  mpz_urandomm(rand,rstate,mpz_tn);
  num=mpz_get_ui(rand);
  gmp_randclear(rstate);
  mpz_clear(rand);
  mpz_clear(mpz_tn);
return (unsigned long)num;
}

void hashing(char *w,mpz_t hash)
{ /* compute hash function */
    char h[32];
    char h2[64];
    sha256 sh;
    shs256_init(&sh);
    for(int i=0;i<strlen(w);i++) shs256_process(&sh,w[i]);
    shs256_hash(&sh,h);
    for(int i=0;i<32;i++)
      sprintf(&h2[i*2],"%02x",(unsigned char)h[i]);
   mpz_init_set_str(hash,h2,16);
}
 
bool Bverify(char filename[100][50],mpz_t T,mpz_t p[100],mpz_t sk[100],mpz_t a[100][100],int c,mpz_t F[100][512],mpz_t g[100][512],long v[100][100],long n[],int usernum,mpz_t mp){
mpz_t sum;
mpz_init_set_str(sum,"1",10);
for(int l=0;l<usernum;l++){
mpz_t A;
mpz_t B;
mpz_t suml;
mpz_init(suml);
mpz_init_set_str(A,"1",10);
mpz_init_set_str(B,"1",10);
for(int i=0;i<c;i++){
mpz_t hash;
mpz_t temp1;
mpz_t temp2;
mpz_init(temp1);
mpz_init(temp2);
char w[15];
sprintf(w,"%s%ld%d%ld",filename[l],n[l],512,v[l][i]);
hashing(w,hash);
mpz_mul(temp1,a[l][i],sk[l]);
mpz_powm(temp2,hash,temp1,p[l]);
mpz_mul(A,A,temp2);
mpz_clear(hash);
mpz_clear(temp1);
mpz_clear(temp2);
}
mpz_mod(A,A,p[l]);
for(int i=0;i<512;i++)
{
mpz_t temp1;
mpz_t temp2;
mpz_init(temp1);
mpz_init(temp2);
mpz_mul(temp1,F[l][i],sk[l]);
mpz_powm(temp2,g[l][i],temp1,p[l]);
mpz_mul(B,B,temp2);
mpz_clear(temp1);
mpz_clear(temp2);
}
mpz_mod(B,B,p[l]);
mpz_mul(suml,A,B);
mpz_mod(suml,suml,p[l]);
mpz_mod(suml,suml,mp);
mpz_mul(sum,sum,suml);
mpz_clear(A);
mpz_clear(B);
mpz_clear(suml);
}
mpz_mod(sum,sum,mp);
gmp_printf("sum=%Zd\n",sum);
gmp_printf("T=%Zd\n",T);
int flag=mpz_cmp(sum,T);
mpz_clear(sum);
if(flag==0)
  return true;
else
  return false;
}
void mpGen(mpz_t mp,int qbits){
                //initial mp
                mpz_init(mp);
                //random state initialization
                gmp_randstate_t rstate;
                gmp_randinit_default(rstate);
                //set an initial seed value into state
                gmp_randseed_ui(rstate,time(NULL));
                while(1){
                mpz_urandomb(mp,rstate,qbits);
                mpz_nextprime(mp,mp);
                int res;
                res=mpz_probab_prime_p(mp,10);
                if(res!=0)
                    break;    
                }
                gmp_randclear(rstate);
}

int main(int argc,char* argv[]){  
  char* strc=argv[1];
  int c=atoi(strc);
  //int c=10;
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  char* hosts = "127.0.0.1";

  cluster = create_cluster(hosts);

   if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }
 
  mpz_t mp;
  char username[100][50];
  char filename[100][50];
  int usernum=0;
  select_from_userkey_user(session,username,filename,&usernum);
  mpGen(mp,1024);
  //int c=atoi(strc);
  mpz_t a[100][c];
  long v[100][c];
  mpz_t T;
  mpz_t F[100][512];
  mpz_t mpz_tp[100];
  mpz_t mpz_tq[100];
  mpz_t mpz_tsk[100];
  mpz_t mpz_tg[100][512];
  long n[100]={0};
  mpz_init_set_str(T,"1",10);
 clock_t timesum=0,avtime=0;
 clock_t start,end;
 start=clock();
 for(int l=0;l<usernum;l++){
  char* p;//=(char*)malloc(sizeof(char)*500);
  char* sk;//=(char*)malloc(sizeof(char)*500);
  char* q;//(char*)malloc(sizeof(char)*300);
  char* g[512];
  mpz_t k1,k2;
  char* file[512];
  char* file2[512];
  mpz_t Tl;
  /*for(int i=0;i<512;i++){
    g[i]=(char*)malloc(sizeof(char)*1024);
}*/
  clock_t start3,end3;
  start3=clock();
  select_from_userdata_n(session,username[l],filename[l],&n[l]);
  //printf("n=%ld\n",n[l]);
  select_from_userkey(session,username[l],filename[l],&p,&q,&sk,g);
  end3=clock();
  timesum+=(end3-start3);
  for(int i=0;i<512;i++){
    mpz_init_set_str(mpz_tg[l][i],g[i],10);
}
  mpz_init_set_str(mpz_tq[l],q,10);
  mpz_init_set_str(mpz_tp[l],p,10);
  mpz_init_set_str(mpz_tsk[l],sk,10);
  end3=clock();
  timesum+=(end3-start3);
  mpz_init(k1);
  mpz_init(k2);
  //gmp_printf("%s\n",q);
  clock_t start4,end4;
  start4=clock();
  challenge(k1,k2,q);
  end4=clock();
  timesum+=(end4-start4);
  clock_t start5,end5;
  start5=clock();
  for(long i=0;i<c;i++){
   mpz_init(a[l][i]); 
   PRF(k1,i,a[l][i],q);
   v[l][i]=PRP(k2,i,n[l]);
}
  end5=clock();
  avtime=end5-start5;
   clock_t start7,end7;
    start7=clock();
    for(int k=0;k<512;k++)
     {
        file[k]=(char*)malloc(sizeof(char)*33);
        file2[k]=(char*)malloc(sizeof(char)*65);
     }
    end7=clock();
    timesum+=(end7-start7);
  for(int j=0;j<512;j++){
    mpz_init(F[l][j]);
    for(int i=0;i<c;i++)
     {
       mpz_t file3[512];
       clock_t start1,end1;
       start1=clock();
       select_from_userdata(session,username[l],filename[l],v[l][i],file);
       bytetoHexstr(file[j],&file2[j],32);
       mpz_init_set_str(file3[j],file2[j],16);
       end1=clock();
       timesum+=(end1-start1);
       mpz_t temp;
       mpz_init(temp);
       mpz_mul(temp,a[l][i],file3[j]);
       mpz_add(F[l][j],F[l][j],temp);
       mpz_clear(file3[j]);
       mpz_clear(temp);
     }
     mpz_mod(F[l][j],F[l][j],mpz_tq[l]);
}
 mpz_init_set_str(Tl,"1",10);
 for(int i=0;i<c;i++)
  {
   char* strtag=(char*)malloc(350);
   mpz_t tag;
   clock_t start2,end2;
   start2=clock();
   select_from_usertag(session,username[l],filename[l],v[l][i],&strtag);
   mpz_init_set_str(tag,strtag,10);
   end2=clock();
   timesum+=(end2-start2);
   mpz_t temp;
   mpz_init(temp);
   mpz_powm(temp,tag,a[l][i],mpz_tp[l]);
   mpz_mul(Tl,Tl,temp);
   mpz_clear(temp);
   mpz_clear(tag);
   free((void*)strtag);
  }
  mpz_mod(Tl,Tl,mpz_tp[l]);
  mpz_mod(Tl,Tl,mp);
  mpz_mul(T,T,Tl);
  
  clock_t start8,end8;
  start8=clock();
  free((void*)p);
  p=NULL;
  free((void*)sk);
  sk=NULL;
  free((void*)q);
  q=NULL;
  mpz_clear(k1);
  mpz_clear(k2);
  for(int i=0;i<512;i++){
    free((void*)g[i]);
    g[i]=NULL;
    free((void*)file[i]);
    file[i]=NULL;
    free((void*)file2[i]);
    file2[i]=NULL;
   }
  end8=clock();
  timesum+=(end8-start8);
}
  mpz_mod(T,T,mp);
  end=clock();
  printf("证据生成耗时：%f\n",(double)(end-start-timesum)/CLOCKS_PER_SEC);
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);
  clock_t start6,end6;
  start6=clock();
  bool flag=Bverify(filename,T,mpz_tp,mpz_tsk,a,c,F,mpz_tg,v,n,usernum,mp);
  end6=clock();
  printf("验证耗时：%f\n",(double)(end6-start6+avtime)/CLOCKS_PER_SEC);
  if(flag)
    puts("验证通过");
  else
    puts("验证没有通过");
for(int l=0;l<usernum;l++){
  mpz_clear(mpz_tq[l]);
  mpz_clear(mpz_tp[l]);
  for(int i=0;i<512;i++)
    {
     mpz_clear(mpz_tg[l][i]);
     mpz_clear(F[l][i]);
    }
  for(int i=0;i<c;i++)
    mpz_clear(a[l][i]);
}
  mpz_clear(T);
  mpz_clear(mp);

  /*close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);*/
  return 0;
}
