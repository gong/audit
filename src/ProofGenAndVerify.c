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
  const char* ptemp;
  const char* sktemp;
  const char* qtemp;
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
     // *p=(char*)malloc(psize+1);
      memcpy(*p,ptemp,psize);

      cass_value_get_string(qvalue,&qtemp,&qsize);
      //*q=(char*)malloc(qsize+1);
      memcpy(*q,qtemp,qsize);

      cass_value_get_string(skvalue,&sktemp,&sksize);
      //*sk=(char*)malloc(sksize+1);
      memcpy(*sk,sktemp,sksize);

      g_iterator=cass_iterator_from_collection(gvalue);
      int i=0;
      while (cass_iterator_next(g_iterator)) {
        const char* strg=NULL;
        size_t strg_length;
        cass_value_get_string(cass_iterator_get_value(g_iterator), &strg, &strg_length);
        //printf("g: %.*s\n", (int)strg_length, strg);
        memcpy(g[i++],strg,strg_length);
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
  
bool verify(char* filename,mpz_t T,mpz_t p,mpz_t sk,mpz_t a[],int c,mpz_t F[],mpz_t g[],long v[],long n){
mpz_t A;
mpz_t B;
mpz_t sum;
mpz_init(sum);
mpz_init_set_str(A,"1",10);
mpz_init_set_str(B,"1",10);
for(int i=0;i<c;i++){
mpz_t hash;
mpz_t temp1;
mpz_t temp2;
mpz_init(temp1);
mpz_init(temp2);
char w[15];
sprintf(w,"%s%ld%d%ld",filename,n,512,v[i]);
hashing(w,hash);
mpz_mul(temp1,a[i],sk);
mpz_powm(temp2,hash,temp1,p);
mpz_mul(A,A,temp2);
mpz_clear(hash);
mpz_clear(temp1);
mpz_clear(temp2);
}
mpz_mod(A,A,p);
for(int i=0;i<512;i++)
{
mpz_t temp1;
mpz_t temp2;
mpz_init(temp1);
mpz_init(temp2);
mpz_mul(temp1,F[i],sk);
mpz_powm(temp2,g[i],temp1,p);
mpz_mul(B,B,temp2);
mpz_clear(temp1);
mpz_clear(temp2);
}
mpz_mod(B,B,p);
mpz_mul(sum,A,B);
mpz_mod(sum,sum,p);
gmp_printf("sum=%Zd\n",sum);
gmp_printf("T=%Zd\n",T);
int flag=mpz_cmp(sum,T);
mpz_clear(A);
mpz_clear(B);
mpz_clear(sum);
if(flag==0)
  return true;
else
  return false;
}
int main(int argc,char* argv[]){
  char* username=argv[1];
  char* filename=argv[2];
  char* strc=argv[3];
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
  char* p=(char*)malloc(sizeof(char)*500);
  char* sk=(char*)malloc(sizeof(char)*500);
  char* q=(char*)malloc(sizeof(char)*300);
  char* g[512];
  int c=atoi(strc);
  mpz_t k1;
  mpz_t k2;
  mpz_t a[c];
  mpz_t mpz_tg[512];
  long v[c];
  mpz_t F[512];
  mpz_t T;
  mpz_t mpz_tq;
  mpz_t mpz_tp;
  mpz_t mpz_tsk;
  char* file[512];
  char* file2[512];
  long n=0;
  for(int i=0;i<512;i++){
    g[i]=(char*)malloc(sizeof(char)*1024);
}
  select_from_userdata_n(session,username,filename,&n);
  select_from_userkey(session,username,filename,&p,&q,&sk,g);
  for(int i=0;i<512;i++){
    mpz_init_set_str(mpz_tg[i],g[i],10);
}
  mpz_init_set_str(mpz_tq,q,10);
  mpz_init_set_str(mpz_tp,p,10);
  mpz_init_set_str(mpz_tsk,sk,10);
  mpz_init(k1);
  mpz_init(k2);
  challenge(k1,k2,q);
  clock_t start1,end1,avtime;
  start1=clock();
  for(long i=0;i<c;i++){
   mpz_init(a[i]); 
   PRF(k1,i,a[i],q);
   v[i]=PRP(k2,i,n);
}
 end1=clock();
 avtime=end1-start1;
    for(int k=0;k<512;k++)
     {
        file[k]=(char*)malloc(sizeof(char)*33);
        file2[k]=(char*)malloc(sizeof(char)*65);
     }
  clock_t startT,endT,sum;
  double totalT;
  startT=clock();
  for(int j=0;j<512;j++){
    mpz_init(F[j]);
    for(int i=0;i<c;i++)
     {
       mpz_t file3[512];
       clock_t start2,end2;
       start2=clock();
       select_from_userdata(session,username,filename,v[i],file);
       bytetoHexstr(file[j],&file2[j],32);
       mpz_init_set_str(file3[j],file2[j],16);
       mpz_t temp;
       mpz_init(temp);
       end2=clock();
       sum+=(end2-start2);
       mpz_mul(temp,a[i],file3[j]);
       mpz_add(F[j],F[j],temp);
       mpz_clear(file3[j]);
       mpz_clear(temp);
     }
     mpz_mod(F[j],F[j],mpz_tq);
}
 mpz_init_set_str(T,"1",10);
 for(int i=0;i<c;i++)
  {
   char* strtag=(char*)malloc(350);
   mpz_t tag;
   clock_t start3,end3;
   start3=clock();
   select_from_usertag(session,username,filename,v[i],&strtag);
   mpz_init_set_str(tag,strtag,10);
   mpz_t temp;
   mpz_init(temp);
    end3=clock();
   sum+=(end3-start3);
   mpz_powm(temp,tag,a[i],mpz_tp);
   mpz_mul(T,T,temp);
   mpz_clear(temp);
   mpz_clear(tag);
   free((void*)strtag);
  }
  mpz_mod(T,T,mpz_tp);
  endT=clock();
  totalT=(double)(endT-startT-sum+avtime)/CLOCKS_PER_SEC;
  printf("证据耗时：%fs\n",totalT);

  clock_t start,end;
  double total;
  start=clock();
  bool flag=verify(filename,T,mpz_tp,mpz_tsk,a,c,F,mpz_tg,v,n);
  end=clock();
  total=(double)(end-start+avtime)/CLOCKS_PER_SEC;
  printf("验证耗时:%fs\n",total);
  if(flag)
    puts("验证通过");
  else
    puts("验证没有通过");
  free((void*)p);
  free((void*)sk);
  free((void*)q);
  for(int i=0;i<512;i++)
    {
     free((void*)g[i]);
     mpz_clear(mpz_tg[i]);
     mpz_clear(F[i]);
    free((void*)file[i]);
    file[i]=NULL;
    free((void*)file2[i]);
    file2[i]=NULL;
    }
  mpz_clear(T);
  mpz_clear(k1);
  mpz_clear(k2);
  mpz_clear(mpz_tq);
  mpz_clear(mpz_tp);
  for(int i=0;i<c;i++)
    mpz_clear(a[i]);
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;

}
