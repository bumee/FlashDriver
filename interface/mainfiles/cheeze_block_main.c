#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <signal.h>
#include "../../include/FS.h"
#include "../../include/settings.h"
#include "../../include/types.h"
#include "../../bench/bench.h"
#include "../interface.h"
#include "../vectored_interface.h"
#include "../cheeze_hg_block.h"

void log_print(int sig){
	free_cheeze();
	printf("f cheeze\n");
	inf_free();
	printf("f inf\n");
	fflush(stdout);
	fflush(stderr);
	sync();
	printf("before exit\n");
	exit(1);
}

void * thread_test(void *){
	vec_request *req=NULL;
	while((req=get_vectored_request())){
		assign_vectored_req(req);
	}
	return NULL;
}

pthread_t thr; 
int main(int argc,char* argv[]){
	struct sigaction sa;
	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);
	printf("signal add!\n");

	inf_init(1,0,argc,argv);

	if(argc<2){
		init_cheeze(0);
	}
	else{
		init_cheeze(atoll(argv[1]));
	}

	pthread_create(&thr, NULL, thread_test, NULL);
	pthread_join(thr, NULL);

	free_cheeze();
	inf_free();
	return 0;
}
