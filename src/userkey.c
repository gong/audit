#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<gmp.h>
#include<cassandra.h>
#include<time.h>

void print_error(CassFuture* future){
 const char* message;
 size_t message_length;
 cass_future_error_message(future,&message,&message_length);
 fprintf(stderr,"Error:%.*s\n",(int)message_length,message);
}
CassCluster* create_cluster(const char* hosts){
 CassCluster* cluster=cass_cluster_new();
 cass_cluster_set_contact_points(cluster,hosts);
 return cluster;
}
CassError connect_session(CassSession* session,const CassCluster* cluster){
 CassError rc=CASS_OK;
 CassFuture* future = cass_session_connect(session,cluster);
 
 cass_future_wait(future);
 rc = cass_future_error_code(future);
 if (rc != CASS_OK) {
   print_error(future);
 }
 cass_future_free(future);
 return rc;
}
CassError insert_into_collections(CassSession* session,char* username,char*filename,char* q,char* p,char* g[], char* sk){
 CassError rc=CASS_OK;
 CassStatement* statement = NULL;
 CassFuture* future = NULL;
 CassCollection* list =NULL;
 char** item=NULL;
 char* query = "INSERT INTO myproject.userkey(username,filename,g,p,q,sk) VALUES(?,?,?,?,?,?);";
 statement = cass_statement_new(query,6);
 
 cass_statement_bind_string(statement,0,username);
 cass_statement_bind_string(statement,1,filename);
 cass_statement_bind_string(statement,3,p);
 cass_statement_bind_string(statement,4,q);
 cass_statement_bind_string(statement,5,sk);

 list=cass_collection_new(CASS_COLLECTION_TYPE_LIST,512);
 for(item=g;*item;item++) {
  cass_collection_append_string(list,*item);
 }
 cass_statement_bind_collection(statement,2,list);
 cass_collection_free(list);

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

unsigned long int RSHash(char* str,unsigned int len){
        unsigned long int b=378551;
        unsigned long int a=63689;
        unsigned long int hash=0;
        unsigned int i=0;
        for(i=0;i<len;str++,i++){
                hash=hash*a+(*str);
                a=a*b;
        }
        //printf("hash=%lu\n",hash);
        return hash;
}

void qGen(mpz_t q,int qbits,unsigned long int seed){
                clock_t startT,endT;
                double totalT;
                startT=clock();
                //initial q
                mpz_init(q);
                //random state initialization
                gmp_randstate_t rstate;
                gmp_randinit_default(rstate);
                //set an initial seed value into state
                gmp_randseed_ui(rstate,seed);
                while(1){
                mpz_urandomb(q,rstate,qbits);
                mpz_nextprime(q,q);
                int res;
                res=mpz_probab_prime_p(q,10);
                if(res!=0){
                        endT=clock();
                        totalT=(double)(endT-startT)/CLOCKS_PER_SEC;
                        printf("q generate time:%fs\n",totalT);
                        break;
                        }
                }
                gmp_randclear(rstate);
}

void pGen(mpz_t p,mpz_t q,int pbits,unsigned long int seed){
        clock_t startT,endT;
        double totalT;
        startT=clock();
        mpz_t rop,r;
        mpz_init(rop);
        mpz_init(r);
        mpz_mul_si(rop,q,2);

        //initial p
         mpz_init(p);
         //random state initialization
         gmp_randstate_t rstate;
         gmp_randinit_default(rstate);
         //set an initial seed value into state
         gmp_randseed_ui(rstate,seed);
         while(1){
                mpz_urandomb(p,rstate,pbits);

                //gmp_printf("p=%Zd\n",p);
                mpz_mod(r,p,rop);
                //gmp_printf("r=%Zd\n",r);
                mpz_sub(p,p,r);
                //gmp_printf("sub_p=%Zd\n",p);
                mpz_add_ui(p,p,1);
                //gmp_printf("add_p=%Zd\n",p);
                int res;
                res=mpz_probab_prime_p(p,10);
                if(res!=0){
                        endT=clock();
                        totalT=(double)(endT-startT)/CLOCKS_PER_SEC;
                        printf("p generate time:%fs\n",totalT);
                        break;
                        }
        }
        mpz_clear(rop);
        mpz_clear(r);
        gmp_randclear(rstate);
}

void HKeyGen(mpz_t p,mpz_t q,mpz_t g[512],int pbits,int qbits,int m,unsigned long int seed){
        qGen(q,qbits,seed);
        pGen(p,q,pbits,seed);
        mpz_t x;
        mpz_t exp;
        mpz_t temp;
        mpz_init(exp);
        mpz_init(temp);
        //mpz_array_init(g,512,qbits);
        mpz_set(temp,p);
        mpz_sub_ui(temp,temp,1);
        mpz_divexact(exp,temp,q);
        mpz_init(x);
        gmp_randstate_t rstate;
        gmp_randinit_default(rstate);
        gmp_randseed_ui(rstate,seed);

        for(int i=0;i<m;i++){
                mpz_init(g[i]);
                do{
                mpz_urandomm(x,rstate,temp);
                mpz_add_ui(x,x,1);
                mpz_powm(g[i],x,exp,p);
                }while(mpz_cmp_ui(g[i],1)==0);
        }
        mpz_clear(x);
        mpz_clear(exp);
        mpz_clear(temp);
        gmp_randclear(rstate);
}

void skGen(mpz_t p,mpz_t sk){
   gmp_randstate_t rstate;
   gmp_randinit_default(rstate);
   gmp_randseed_ui(rstate,time(NULL));
   mpz_urandomm(sk,rstate,p);
   gmp_randclear(rstate);
}
int main(int argc,char* argv[]){
 char* username=argv[1];
 char* filename=argv[2];
 CassCluster* cluster =NULL;
 CassSession* session =cass_session_new();
 CassFuture* close_future=NULL;
 char* host = "192.168.247.138";
 
  mpz_t q,p,g[512],sk;
  unsigned long int seed;
  seed=RSHash(filename,strlen(filename));
  HKeyGen(p,q,g,1024,257,512,seed);
  skGen(p,sk);
  char* strq=malloc(mpz_sizeinbase (q, 10) + 2);
  mpz_get_str(strq,10,q);
  char* strp=malloc(mpz_sizeinbase (p, 10) + 2);
  mpz_get_str(strp,10,p);
  char* strg[513]={"0"};
  char* strsk=malloc(mpz_sizeinbase (sk, 10) + 2);
  mpz_get_str(strsk,10,sk);
  for(int i=0;i<512;i++){
     strg[i]=malloc(mpz_sizeinbase (g[i], 10) + 2);
      mpz_get_str(strg[i],10,g[i]);
    }
  // if(argc>1){
   // host = argv[1];
 //}
 cluster=create_cluster(host);
 if (connect_session(session,cluster)!=CASS_OK){
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
 }
 cass_cluster_set_write_bytes_high_water_mark(cluster,131072);
 insert_into_collections(session,username,filename,strq,strp,strg,strsk);
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);
   
   mpz_clear(p);
   mpz_clear(q);
   mpz_clear(sk);
   free(strp);
   free(strq);
   free(strsk);
   for(int i=0;i<512;i++){
      mpz_clear(g[i]); 
      free((void*)strg[i]);
     }
return 0;
}
