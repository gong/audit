#include<stdio.h>
#include<cassandra.h>
#include<stdlib.h>
#include<gmp.h>
#include<time.h>
#include<string.h>
#include<miracl/miracl.h>

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
        memcpy(file[i++],strfile,strfile_length);
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


CassError select_from_userkey(CassSession* session,char* username,char* filename,char** p,char** sk,char* g[]) {
  const char* ptemp;
  const char* sktemp;
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT p,sk,g FROM myproject.userkey WHERE  username= ? and filename= ?";

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
      const CassValue* skvalue=NULL;
      const CassValue* gvalue=NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      CassIterator* g_iterator = NULL;

      size_t psize,sksize;
      pvalue=cass_row_get_column(row, 0);
      skvalue=cass_row_get_column(row,1);
      gvalue=cass_row_get_column(row,2);
      cass_value_get_string(pvalue,&ptemp,&psize);
      memcpy(*p,ptemp,psize);
      cass_value_get_string(skvalue,&sktemp,&sksize);
      memcpy(*sk,sktemp,sksize);
      g_iterator=cass_iterator_from_collection(gvalue);
      int i=0;
      while (cass_iterator_next(g_iterator)) {
        const char* strg=NULL;
        size_t strg_length;
        cass_value_get_string(cass_iterator_get_value(g_iterator), &strg, &strg_length);
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
   mpz_set_str(hash,h2,16);
}

CassError insert_into_usertag(CassSession* session,char* username,char*filename,int blocknum, char* tag){
 CassError rc=CASS_OK;
 CassStatement* statement = NULL;
 CassFuture* future = NULL;
 CassCollection* list =NULL;
 char** item=NULL;
 char* query = "INSERT INTO myproject.usertag(username,filename,blocknum,tag) VALUES(?,?,?,?);";
 statement = cass_statement_new(query,4);

 cass_statement_bind_string(statement,0,username);
 cass_statement_bind_string(statement,1,filename);
 cass_statement_bind_int32(statement,2,blocknum);
 cass_statement_bind_string(statement,3,tag);

 future=cass_session_execute(session,statement);
 cass_future_wait(future);

 rc=cass_future_error_code(future);
 if(rc!= CASS_OK) {
  print_error(future);
 }
 cass_future_free(future);
 cass_statement_free(statement);
 return rc;
}


int main(int argc,char* argv[]){
  char* username=argv[1];
  char* filename=argv[2];
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  char* hosts = "192.168.247.138";
  
  cluster = create_cluster(hosts);
  
   if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  char* p=(char*)malloc(sizeof(char)*500);
  char* sk=(char*)malloc(sizeof(char)*500);
  char* g[512];
  long n;
  for(int i=0;i<512;i++)
    g[i]=(char*)malloc(sizeof(char)*500);
  
  select_from_userdata_n(session,username,filename,&n);
  select_from_userkey(session,username,filename,&p,&sk,g);
  char* file[512];
  char* file2[512];
  mpz_t file3[512];
  mpz_t mpz_tg[512];
  mpz_t mpz_tsk;
  mpz_t mpz_tp;
  mpz_init_set_str(mpz_tp,p,10);
  mpz_init_set_str(mpz_tsk,sk,10);
 for(int i=0;i<512;i++)
   {
     file[i]=(char*)malloc(sizeof(char)*33);
     file2[i]=(char*)malloc(sizeof(char)*65);
     mpz_init_set_str(mpz_tg[i],g[i],10);
     mpz_init(file3[i]);
  }
  clock_t sumquery=0;

 for(int j=0;j<n;j++){
  mpz_t hash;
  mpz_t sum;
  mpz_init(hash);
  mpz_init(sum);
  char w[15];
  sprintf(w,"%s%ld%d%d",filename,n,512,j);
  select_from_userdata(session,username,filename,j,file);
  clock_t startT,endT;
  double totalT;
  startT=clock(); 
  hashing(w,hash);
  mpz_powm(sum,hash,mpz_tsk,mpz_tp);
  for(int i=0;i<512;i++)
  {
    mpz_t temp1,temp2;
    mpz_init(temp1);
    mpz_init(temp2);
    bytetoHexstr(file[i],&file2[i],32);
   // printf("-----二级块%dfile%d:%s\n",j,i,file2[i]);
   mpz_set_str(file3[i],file2[i],16);
   mpz_mul(temp1,mpz_tsk,file3[i]);
   mpz_powm(temp2,mpz_tg[i],temp1,mpz_tp);
   mpz_mul(sum,sum,temp2);
   mpz_clear(temp1);
   mpz_clear(temp2);
   }
  mpz_mod(sum,sum,mpz_tp);
  endT=clock();
  sumquery+=(endT-startT);
  totalT=(double)(endT-startT)/CLOCKS_PER_SEC;
  //gmp_printf("标签%d:%Zd\n",j,sum);
  printf("生成标签时间：%fs\n",totalT);
  char* result=malloc(mpz_sizeinbase (sum, 10) + 2);
  mpz_get_str(result,10,sum);
  insert_into_usertag(session,username,filename,j,result);
   mpz_clear(hash);
   mpz_clear(sum);
   free((void*)result);
}
   printf("总生成标签时间：%fs\n",(double)sumquery/CLOCKS_PER_SEC);
   for(int i=0;i<512;i++)
    {
     free((void*)file[i]);
     file[i]=NULL;
     free((void*)file2[i]);
     file2[i]=NULL;
    }

  mpz_clear(mpz_tsk);
  mpz_clear(mpz_tp);
  for(int i=0;i<512;i++)
  {
    mpz_clear(file3[i]);
    mpz_clear(mpz_tg[i]);
    free(g[i]);
  }
  free((void*)p);
  free((void*)sk);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;

}
