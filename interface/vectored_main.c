#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <getopt.h>
#include "../include/lsm_settings.h"
#include "../include/FS.h"
#include "../include/settings.h"
#include "../include/types.h"
#include "../bench/bench.h"
#include "interface.h"
#include "vectored_interface.h"
#include "../include/utils/kvssd.h"
extern int req_cnt_test;
extern uint64_t dm_intr_cnt;
extern int LOCALITY;
extern float TARGETRATIO;
extern int KEYLENGTH;
extern int VALUESIZE;
extern uint32_t INPUTREQNUM;
extern master *_master;
extern bool force_write_start;
extern int seq_padding_opt;
MeasureTime write_opt_time[11];
extern master_processor mp;
extern uint64_t cumulative_type_cnt[LREQ_TYPE_NUM];

bool islob = false;
uint32_t start = 0;
uint32_t end = 0;
uint64_t number = 0;

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void process_large_csv_to_bench(const char *csv_file_path) {
    FILE *file = fopen(csv_file_path, "r");
    if (!file) {
        perror("Failed to open CSV file");
        exit(EXIT_FAILURE);
    }

    char line[4096]; // CSV 파일의 한 줄 버퍼
    int line_count = 0;

    // 첫 번째 라인을 읽어서 헤더를 건너뜀
    if (!fgets(line, sizeof(line), file)) {
        fprintf(stderr, "Error: Failed to read the header line\n");
        fclose(file);
        return;
    }

    // CSV 파일의 각 행을 한 줄씩 읽음
    while (fgets(line, sizeof(line), file)) {
        line_count++;

        // 첫 번째 행에서 시작 키 값 설정
        if (line_count == 1) {
            char *token = strtok(line, ",");
            if (token == NULL) {
                fprintf(stderr, "Error: Failed to parse start key\n");
                fclose(file);
                return;
            }
            start = (uint32_t)atoi(token);
        }

        // 마지막 행의 키 값을 계속 갱신하여 최종적으로 end에 저장
        char *last_token = strtok(line, ",");
        while (last_token != NULL) {
            last_token = strtok(NULL, ",");
        }
        if (last_token != NULL) {
            end = (uint32_t)atoi(last_token);
        }
    }

    // 요청 개수는 총 라인 수
    // 제한된 데이터까지만 처리
	number = (line_count / 1024) * 1024; // 1024의 배수로 맞춤

    fclose(file);

    printf("Processed %d lines from CSV file\n", line_count);
}


int main(int argc,char* argv[]){
	process_large_csv_to_bench("ETH_1min.csv");

	//int temp_cnt=bench_set_params(argc,argv,temp_argv);
	inf_init(0,0,argc,argv);
	bench_init();
	bench_vectored_configure();
	bench_add(VECTOREDLOBSET,0,number,number);
    bench_add(VECTOREDSGET,0,number,number);
	// bench_add(VECTOREDSGET,0,number,number);
	// bench_add(VECTOREDRW,0,RANGE,RANGE*2);
	// bench_add(VECTOREDRGET,0,RANGE/100*99,RANGE/100*99);
	// printf("range: %lu!\n",RANGE);
	printf("range: %lu!\n", number);
	//bench_add(VECTOREDRW,0,RANGE,RANGE*2);


	char *value;
	uint32_t mark;
	while((value=get_vectored_bench(&mark))){
		inf_vector_make_req(value, bench_transaction_end_req, mark, islob=true);
	}

	force_write_start=true;
	
	printf("bench finish\n");
	while(!bench_is_finish()){
#ifdef LEAKCHECK
		sleep(1);
#endif
	}

	inf_free();
	bench_custom_print(write_opt_time,11);
	return 0;
}
