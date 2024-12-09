/***********************************************
 * Project 4: Map-Reduce
 *
 * Group Number :
 * Students     :
 *
 * Please add details if compilation of your project is not
 * straightforward (for instance, if you use a different
 * structure than outlined in the project statement).
 ***********************************************/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}



void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL){
        count++;
        //printf("Got value: %s for key: %s\n", value, key);
        free(value);
    }

    printf("%s %d\n", key, count); // word "key" appears "count" times
}



int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 8, Reduce, 8, MR_DefaultHashPartition);
}
