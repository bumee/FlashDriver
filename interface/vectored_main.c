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
#include <cjson/cJSON.h>

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
uint64_t number = 0;
char **buffers;
size_t *buffer_sizes;
int buffer_count;

void csv_to_json_split_buffers(const char *csv_file_path, char **buffers[], size_t *buffer_sizes, int *buffer_count) {
    FILE *csv_file = fopen(csv_file_path, "r");
    // FILE *json_file = fopen(json_file_path, "w");

    if (!csv_file) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }

    char line[4096]; // line buffer
    char *headers[256]; // buffer for headers
    int header_count = 0;
    int linkcount = 0;

    // header read
    if (fgets(line, sizeof(line), csv_file)) {
        char *token = strtok(line, ",\n"); // separate of comma and \n
        while (token) {
            headers[header_count++] = strdup(token); // header store
            token = strtok(NULL, ",\n");
        }
    }

    // start JSON
    // fprintf(json_file, "[\n");

    int is_first_row = 1; // is first row?
    int current_buffer_index = 0;

    size_t buffer_capacity = 1024*K; // buffer initialization (1MB)
    *buffers = (char **)malloc(sizeof(char *) * 20000); // maximum buffer count
    buffer_sizes = (size_t *)calloc(20000, sizeof(size_t));
    *buffer_count = 1;

    (*buffers)[current_buffer_index] = (char *)malloc(buffer_capacity);
    buffer_sizes[current_buffer_index] = 0;

    // data handling
    while (fgets(line, sizeof(line), csv_file)) {
        linkcount++;
        if (!is_first_row) {
            // fprintf(json_file, ",\n");
            snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], ",\n");
            buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);
        }
        // fprintf(json_file, "  {\n");
        snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "  {\n");
        buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);

        char *token = strtok(line, ",\n"); // read data
        token = strtok(NULL, ",\n"); // 첫 번째 index 값 무시하고 system time부터 받아오기 시작
        for (int i = 0; i < header_count; i++) {
            // fprintf(json_file, "    \"%s\": ", headers[i]);
            snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "    \"%s\": ", headers[i]);
            buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);

            if (token) {
                // 숫자인지 문자열인지 판별
                if (strspn(token, "0123456789.") == strlen(token)) {
                    // fprintf(json_file, "%s", token); // 숫자 그대로 출력
                    snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "%s", token);
                    buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);
                } else {
                    // fprintf(json_file, "\"%s\"", token); // 문자열로 출력
                    snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "\"%s\"", token);
                    buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);
                }
                token = strtok(NULL, ",\n");
            } else {
                // fprintf(json_file, "null"); // 누락된 값 처리
                snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "null");
                buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);
            }
            if (i < header_count - 1) {
                // fprintf(json_file, ",");
                snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], ",");
                buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);
            }
            // fprintf(json_file, "\n");
            snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "\n");
            buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);
        }
        // fprintf(json_file, "  }");
        snprintf((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index], buffer_capacity - buffer_sizes[current_buffer_index], "  }");
        buffer_sizes[current_buffer_index] += strlen((*buffers)[current_buffer_index] + buffer_sizes[current_buffer_index]);

        // `}`마다 새로운 buffer로 변경
        current_buffer_index++;
        (*buffers)[current_buffer_index] = (char *)malloc(buffer_capacity);
        buffer_sizes[current_buffer_index] = 0;
        (*buffer_count)++;
        is_first_row = 0;

        // 버퍼 크기 초과 방지
        if (buffer_sizes[current_buffer_index] > buffer_capacity - 4096) {
            buffer_capacity *= 2;
            (*buffers)[current_buffer_index] = (char *)realloc((*buffers)[current_buffer_index], buffer_capacity);
            if (!(*buffers)[current_buffer_index]) {
                perror("Failed to reallocate memory for buffer");
                exit(EXIT_FAILURE);
            }
        }
    }

    // fprintf(json_file, "\n]\n"); // JSON 배열 끝

    // 파일 닫기
    fclose(csv_file);
    // fclose(json_file);

    number = (linkcount / 1024) * 1024;

    // printf("Converted CSV to JSON: %s\n", json_file_path);
    printf("Total Buffers: %d\n", *buffer_count);
}


int main(int argc,char* argv[]){
	csv_to_json_split_buffers("ETH_1min.csv", &buffers, buffer_sizes, &buffer_count);

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
    // 각 buffer에 저장된 JSON free
    for (int i = 0; i < buffer_count; i++) {
        free(buffers[i]); // 메모리 해제
    }
    free(buffers);
    free(buffer_sizes);
	return 0;
}
