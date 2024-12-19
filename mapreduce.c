#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
/************************************************************************************************************************
 * The chosen data structure for this project is a two leveled thread safe linked list                                  *
 * The choice was motivated by the nature of the MapReduce problem,                                                     *
 *   since linked lists allow dynamic inserts, deletes and using a lock makes them safe for multi-thread programs.      *
 * The partition linked list uses nodes of level 1 (entry_t) corresponding to the keys.                                 *
 * The entry_t linked list uses nodes of level 2 (values_t) where the associated values will be stored.                 *
 * The values are directly grouped together in the mapping phase following the next execution :                         *
 *   ->Finding an entry with the same key (O(n) in worst case).                                                         *
 *   ->Inserting the value at the beginning (O(1)).                                                                     *
 * Locks are only used in the mapping phase on a partition tp deal with concurrency, but not                            *
 *   in the reducing phase since a partition is being accessed by one thread only.                                      *
 * One line has been added to the Reduce function to match the code structure thus avoiding memory leaks.               *
 * When getting the next value in the reduce phase, the 2nd level node is deleted and the head points to its next,      *
 *   resulting in a O(1) for every value read.                                                                          *
 ***********************************************************************************************************************/



typedef struct Node {
    char* key;
    char** values;
    struct Node *left;
    struct Node *right;
    int sizeValues;
} Node;


typedef struct partition {
    Node* node;
    pthread_mutex_t lock;
} partition_t;


// Global variables
partition_t *partitions;
int num_partitions;
Partitioner partitioner_; //MrDefaultHash by default


Node * node = NULL;


void free_node(Node* node) {
    if (node == NULL)
        return;

    free_node(node->left);
    free_node(node->right);

    for (int j = 0; j < node->sizeValues; j++) {
        free(node->values[j]);
    }
    free(node->values);
    free(node->key);
    free(node);
}
void cleanup_partitions() {
    for (int i = 0; i < num_partitions; i++) {

        free_node(partitions[i].node);
        pthread_mutex_destroy(&partitions[i].lock);
    }
    free(partitions);
}



//Structure to group argues passed to reduce_
typedef struct reduce_args{
    Reducer  reducer;
    int  partition_num;
}reduce_args_t ;



Node* creerNode(char* key,char *value) {
    Node* node = malloc(sizeof(Node));
    assert(node != NULL);

    node->key = strdup(key);
    node->left = NULL;
    node->right = NULL;
    node->values = malloc(sizeof(char*));
    assert(node->values != NULL);
    node->values[0] = strdup(value);
    node->sizeValues = 1;
    return node;
}
Node * inserer(Node* node,char * key,char* value) {
    if (node == NULL)
        return creerNode(key,value);
    else if (strcmp(node->key,key) == 0) {
        node->values = realloc(node->values,sizeof(char*) * (node->sizeValues + 1));
        assert(node->values != NULL);
        node->values[node->sizeValues] = strdup(value);
        node->sizeValues += 1;
    }
    else if (strcmp(node->key,key) > 0)
        node->left =  inserer(node->left,key,value);

    else
        node->right =  inserer(node->right,key,value);
    return node;
}
Node * trouver(Node * node,char* key) {
    if (node == NULL)
        return NULL;
    else if (strcmp(node->key,key) == 0) {
        return node;
    }
    else if(strcmp(node->key,key) > 0) {
        return trouver(node->left,key);
    }
    else {
        return trouver(node->right,key);
    }
}

char* get_next(char* key, int partition_number) {
    Node* node = partitions[partition_number].node;
    Node* node_key = trouver(node, key);
    if (node_key == NULL || node_key->sizeValues == 0)
        return NULL;

    char* value = node_key->values[node_key->sizeValues - 1];
    node_key->sizeValues -= 1;
    return value;
}

// Fonction récursive pour parcourir l'arbre et réduire
void traverse_and_reduce(Node* node,Reducer reduce,int partition_number) {
    if (node == NULL)
        return;
    reduce(node->key, (Getter)get_next, partition_number);
    traverse_and_reduce(node->left,reduce,partition_number);
    traverse_and_reduce(node->right,reduce,partition_number);
}
// Wrapper for the reduce function to process data after the Map phase
void reduce_(reduce_args_t* args) {
    Reducer reduce = args->reducer;
    int partition_number = args->partition_num;

    traverse_and_reduce(partitions[partition_number].node,reduce,partition_number);
}

void MR_Emit(char* key, char* value) {
    unsigned long partition_number = partitioner_(key, num_partitions);
    pthread_mutex_lock(&partitions[partition_number].lock); // Lock to prevent concurrency issues

    partitions[partition_number].node = inserer(partitions[partition_number].node, key, value);

    pthread_mutex_unlock(&partitions[partition_number].lock);  // Unlock after modification
}



// MR_Run implementation: Runs the Map-Reduce process

void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partitioner) {
    num_partitions = num_reducers;
    partitioner_ = partitioner;

    partitions = malloc(num_partitions * sizeof(partition_t));
    for (int i = 0; i < num_partitions; i++) {
        partitions[i].node = NULL;
        pthread_mutex_init(&partitions[i].lock, NULL);
    }

    // Map phase
    char** mapArgs = malloc((argc - 1) * sizeof(char*));
    pthread_t* mapper_threads = malloc((argc - 1) * sizeof(pthread_t));

    for (int i = 1; i < argc; i++) {
        mapArgs[i - 1] = strdup(argv[i]);
        pthread_create(&mapper_threads[i - 1], NULL,(void*)map, (void*)mapArgs[i - 1]);
    }

    for (int i = 0; i < argc - 1; i++) {
        pthread_join(mapper_threads[i], NULL);
        free(mapArgs[i]);
    }
    free(mapper_threads);
    free(mapArgs);

    // Reduce phase
    reduce_args_t* reduceArgs = malloc(num_reducers * sizeof(reduce_args_t));
    pthread_t* reducer_threads = malloc(num_reducers * sizeof(pthread_t));

    for (int i = 0; i < num_reducers; i++) {
        reduceArgs[i].reducer = reduce;
        reduceArgs[i].partition_num = i;
        pthread_create(&reducer_threads[i], NULL, (void*)reduce_, &reduceArgs[i]);
    }

    // Wait for all reducers to finish
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_threads[i], NULL);
    }

    free(reduceArgs);
    free(reducer_threads);

    // Cleanup
    cleanup_partitions();
}



unsigned long MR_DefaultHashPartition(char* key, int num_partitions_) {
    unsigned long hash = 5381;
    unsigned char c;
    while ((c = *key++) != '\0') {
        hash = hash * 33 + c;
    }
    return hash % num_partitions_;  // Return partition number
}




