#include<stdio.h>
#include<cassandra.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<unistd.h>

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
CassError insert_into_userdata(CassSession* session,char* username,char*filename,int blocknum,char* file[]){
 CassError rc=CASS_OK;
 CassStatement* statement = NULL;
 CassFuture* future = NULL;
 CassCollection* list =NULL;
 char** item=NULL;
 char* query = "INSERT INTO myproject.userdata(filename,blocknum,file,username) VALUES(?,?,?,?);";
 statement = cass_statement_new(query,4);

 cass_statement_bind_string(statement,0,filename);
 cass_statement_bind_int32(statement,1,blocknum);
 cass_statement_bind_string(statement,3,username);
 printf("blocknum=%d\n",blocknum);
 int i=0;
 list=cass_collection_new(CASS_COLLECTION_TYPE_LIST,512);
 for(item=file;*item;item++) {
 // printf("%d\n",i++);
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

int fileread(char* path,char* file[]){
  FILE *stream;
  stream=fopen(path,"rb");
  int i=0,count=0;
  while(1){
 count= fread(file[i++],1,32,stream);
 if(count<32)
   break; 
  }
return i;
}
long filesize(char* path){
  FILE *stream;
  stream=fopen(path,"rb");
  struct stat stateinfo;
  stat(path,&stateinfo);
  return stateinfo.st_size;
}
int main(int argc,char* argv[]){
   char* username=argv[1];
   char* filename=argv[2];
   CassCluster* cluster =NULL;
   CassSession* session =cass_session_new();
   CassFuture* close_future=NULL;
   char* host = "192.168.247.138";
   cluster=create_cluster(host);
   if (connect_session(session,cluster)!=CASS_OK){
     cass_cluster_free(cluster);
     cass_session_free(session);
     return -1;
    }

  int sumsector;
  char path[500];
  sprintf(path,"%s%s%s","/home/gongxin/Desktop/",filename,".txt");
  long size=filesize(path);
  int n=size/32;
  char* file[n+2];
  for(int i=0;i<n+2;i++){
   file[i]=(char*)malloc(sizeof(char*)*33);
  }
  sumsector=fileread(path,file);
  
   int k=0;
   char* strfile[513]={NULL};
   for(int i=0,j=0;i<sumsector;i++)
   {
      puts(file[i]); 
      strfile[j++]=file[i];
      if(j==512||(j<512&&i==sumsector-1))
      {
       insert_into_userdata(session,username,filename,k,strfile);
	   int c=0;
	   char** clear;
	   for(clear=strfile;*clear;clear++){
	     strfile[c++]=NULL;
	   }
       k++;
       j=0;
      }
   }
  printf("总块数：%d\n",k);
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  
  for(int i=0;i<n+2;i++)
     free((void*)file[i]);
  return 0;
}
