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
#include <lz4.h>
#include <zstd.h>

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
char **compression_buffers[50000];
size_t *buffer_sizes;
size_t *comp_buffer_sizes;
int buffer_count;
int comp_buffer_count;
double elapsed_time=0;

void csv_to_json_split_buffers(const char *csv_file_path, char **buffers[], size_t *buffer_sizes, int *buffer_count, comp_type cp) {
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
    int current_comp_buffer_index = 0;

    size_t buffer_capacity = PAGESIZE; // buffer initialization (8KB == PAGESIZE)
    *buffers = (char **)malloc(sizeof(char *) * 187000); // maximum buffer count
    buffer_sizes = (size_t *)calloc(187000, sizeof(size_t));
    *buffer_count = 1;

    (*buffers)[current_buffer_index] = (char *)malloc(buffer_capacity);
    buffer_sizes[current_buffer_index] = 0;

    *compression_buffers = (char **)malloc(sizeof(char *) * 50000); // maximum buffer count
    comp_buffer_sizes = (size_t *)calloc(50000, sizeof(size_t));
    comp_buffer_count = 1;

    (*compression_buffers)[current_comp_buffer_index] = (char *)malloc(buffer_capacity);
    comp_buffer_sizes[current_comp_buffer_index] = 0;

    // data handling
    while (fgets(line, sizeof(line), csv_file)) {
        linkcount++;
        if(linkcount>=150000){
            break;
        }
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

        if (cp < None) {
            const char *json_data = (*buffers)[current_buffer_index];
            int json_size = strlen(json_data) + 1; // 널 문자 포함

            // 압축된 데이터를 저장할 버퍼
            int max_compressed_size;
            int compressed_size;
            char *compressed_data;
            struct timeval start, end;
            switch (cp)
            {
            case LZ4:
                max_compressed_size = LZ4_compressBound(json_size);
                compressed_data = (char *)malloc(max_compressed_size);
                gettimeofday(&start, NULL);
                compressed_size = LZ4_compress_default(json_data, compressed_data, json_size, max_compressed_size);
                gettimeofday(&end, NULL);
                break;
            
            case ZStandard:
                max_compressed_size = ZSTD_compressBound(json_size);
                compressed_data = (char *)malloc(max_compressed_size);
                gettimeofday(&start, NULL);
                compressed_size = ZSTD_compress(compressed_data, max_compressed_size, json_data, json_size, 1); // 압축 레벨 
                gettimeofday(&end, NULL);
                break;
            
            default:
                break;
            }

            // 걸린 시간 계산 (초와 마이크로초를 합산)
            elapsed_time += (end.tv_sec - start.tv_sec) + 
                            (end.tv_usec - start.tv_usec) / 1000000.0;

            if (compressed_size <= 0) {
                fprintf(stderr, "Compression failed\n");
                free(compressed_data);
                return;
            }

            // printf("buffer_capacity: %ld and buffer_sizes[%d]: %ld\n", buffer_capacity, current_comp_buffer_index, comp_buffer_sizes[current_comp_buffer_index]);
            // 기존 데이터 크기 확인 
            if (comp_buffer_sizes[current_comp_buffer_index] + compressed_size <= buffer_capacity) {
                // 기존 버퍼에 충분한 공간이 있다면 데이터 복사
                memcpy((*compression_buffers)[current_comp_buffer_index] + comp_buffer_sizes[current_comp_buffer_index], compressed_data, compressed_size);
                comp_buffer_sizes[current_comp_buffer_index] += compressed_size;
            } else {
                // 충분한 공간이 없으면 새로운 버퍼로 이동
                // 버퍼 인덱스 증가 및 새 버퍼 초기화
                current_comp_buffer_index++;
                (*compression_buffers)[current_comp_buffer_index] = (char *)malloc(buffer_capacity);
                comp_buffer_sizes[current_comp_buffer_index] = 0;
                comp_buffer_count++;

                memcpy((*compression_buffers)[current_comp_buffer_index] + comp_buffer_sizes[current_comp_buffer_index], compressed_data, compressed_size);
                comp_buffer_sizes[current_comp_buffer_index] += compressed_size;
            }

            // 압축된 데이터 메모리 해제
            free(compressed_data);
        }
        current_buffer_index++;
        (*buffers)[current_buffer_index] = (char *)malloc(buffer_capacity);
        buffer_sizes[current_buffer_index] = 0;
        (*buffer_count)++;
        is_first_row = 0;
    }

    // fprintf(json_file, "\n]\n"); // JSON 배열 끝

    // 파일 닫기
    fclose(csv_file);
    // fclose(json_file);

    if(cp >= None)
        number = (*buffer_count / 1024) * 1024;
    else
        number = (comp_buffer_count / 1024) * 1024;
    printf("buffer count: %d and number: %ld\n", *buffer_count, number);

    // printf("Converted CSV to JSON: %s\n", json_file_path);
    printf("Total Buffers: %d\n", *buffer_count);
    printf("Total compression time: %f\n", elapsed_time);
}   



int main(int argc,char* argv[]){
	csv_to_json_split_buffers("BTC_1sec.csv", &buffers, buffer_sizes, &buffer_count, ZStandard);

	//int temp_cnt=bench_set_params(argc,argv,temp_argv);
	inf_init(0,0,argc,argv);
	bench_init();
	bench_vectored_configure();
	bench_add(VECTOREDLOBSET,0,number,number);
    // bench_add(VECTOREDSGET,0,number,number);
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
    sleep(1);
    for (int i = 0; i < buffer_count; i++) {
        free(buffers[i]); // 메모리 해제
    }
    for (int i = 0; i < comp_buffer_count; i++)
    {
        free(compression_buffers[i]);
    }
    free(buffers);
    free(buffer_sizes);
    free(comp_buffer_sizes);
	return 0;
}
