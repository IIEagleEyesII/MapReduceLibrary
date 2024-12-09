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




// Level 2 node (values associated with the key)
typedef struct values {
    char* value;
    struct values* next;
} values_t;

// Level 1 node (entries associated with keys)
typedef struct entry {
    char* key;
    struct entry* next;
    values_t* head;  // Head of the Level 2 node list (values)
} entry_t;

// Linked_List structure for thread safety
typedef struct partition {
    entry_t* head;
    pthread_mutex_t lock;
} partition_t;


// Global variables
partition_t *partitions;
int num_partitions;
Partitioner partitioner_; //MrDefaultHash by default

// Helper function to generate a new entry node
entry_t* generate_entry(char* key) {
    entry_t* entry_node = malloc(sizeof(entry_t));
    entry_node->key = strdup(key);
    entry_node->next = NULL;
    entry_node->head = NULL;
    assert(entry_node);  // Ensure memory allocation was successful
    return entry_node;
}

// Function to get an entry or create a new one for the given key
entry_t* get_entry(char* key) {
    unsigned long partition_number = partitioner_(key, num_partitions);  // Find the corresponding partition
    entry_t* current = partitions[partition_number].head;

    // Traverse the list to find an entry with the key
    while (current) {
        if (strcmp(current->key, key) == 0) {  // Found an entry
            return current;
        }
        current = current->next;
    }

    // If key not found, create a new entry and insert at the beginning
    entry_t* entry_node = generate_entry(key);
    entry_node->next = partitions[partition_number].head;
    partitions[partition_number].head = entry_node;
    return entry_node;
}



char* get_next(char* key, int partition_number) {
    partition_t* partition = &partitions[partition_number];
    entry_t* entry = partition->head;

    while (entry) {
        if (strcmp(entry->key, key) == 0) {
            values_t *value_head = entry->head;
            if (value_head) {
                char* ret_val = strdup(value_head->value); //Will be freed after usage in Reduce
                entry->head = value_head->next;
                free(value_head->value);  // Free the value string
                free(value_head);         // Free the value node
                return ret_val;
            }
            break;
        }
        entry = entry->next;
    }
    return NULL;
}

//Structure to group argues passed to reduce_
typedef struct reduce_args{
    Reducer  reducer;
    int  partition_num;
}reduce_args_t ;

// Wrapper for the reduce function to process data after the Map phase
void reduce_(reduce_args_t * args) {
    Reducer reduce = args->reducer;
    int partition_number = args->partition_num;
    partition_t* partition = &partitions[partition_number];
    entry_t* entry = partition->head;
    while (entry) {
        // Call the reduce function for each entry in the partition
        reduce(entry->key, (Getter)get_next, partition_number);
        entry = entry->next;
    }
    free(args);
}

//To free the memory
void cleanup_partitions() {
    for (int i = 0; i < num_partitions; i++) {
        entry_t* entry = partitions[i].head;
        while (entry) {
            values_t* value = entry->head;
            while (value) {
                values_t* tmp_value = value;
                value = value->next;
                free(tmp_value->value);
                free(tmp_value);
            }
            entry_t* tmp_entry = entry;
            entry = entry->next;
            free(tmp_entry->key);
            free(tmp_entry);
        }
        pthread_mutex_destroy(&partitions[i].lock);
    }
    free(partitions);
}


//Prints the partition's content
__attribute__((unused)) void display_partitions(){
    for(int i=0;i<num_partitions;i++){
        entry_t * entry = partitions[i].head;
        while (entry){
            printf("key : \"%s\", values :",entry->key);
            values_t * value = entry->head;
            while (value){
                printf("\"\t%s\t\"",value->value);
                value=value->next;
            }
            printf("\n");
            entry=entry->next;
        }
    }
}


void MR_Emit(char* key, char* value) {
    unsigned long partition_number = partitioner_(key, num_partitions);
    pthread_mutex_lock(&partitions[partition_number].lock); //Lock to prevent concurrency issues

    // Get or create the entry for the key
    entry_t* entry = get_entry(key);

    // Create a new Level 2 node for the value
    values_t* value_ = malloc(sizeof(values_t));
    value_->value = strdup(value);  // Copy the value
    value_->next = entry->head;     // Insert at the beginning of the Level 2 list
    entry->head = value_;

    pthread_mutex_unlock(&partitions[partition_number].lock);  // Unlock after modification
}

// MR_Run implementation: Runs the Map-Reduce process
void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partitioner) {
    // Initialize partitions and threads
    num_partitions = num_reducers;
    partitioner_ = partitioner;
    partitions = malloc(num_partitions * sizeof(partition_t));
    for (int i = 0; i < num_partitions; i++) {  // Initialize the partitions
        partitions[i].head = NULL;
        pthread_mutex_init(&partitions[i].lock, NULL);  // Initialize partition lock
    }

    pthread_t mapper_threads[num_mappers];  // Initialize mappers
    pthread_t reducer_threads[num_reducers];  // Initialize reducers

    // Map phase
    for (int i = 1; i < argc; i++) {  // evenly handle files to reducers
        pthread_create(&mapper_threads[(i - 1) % num_mappers], NULL, (void *) map, (void *) argv[i]);
    }

    for (int i = 0; i < num_mappers; i++) {  // Wait for all mapper threads to complete their tasks
        pthread_join(mapper_threads[i], NULL);
    }

    //display_partitions();

    for (int i = 0; i < num_reducers; i++) {
        reduce_args_t * reduceArgs = malloc(sizeof(reduce_args_t));
        reduceArgs->reducer = reduce;
        reduceArgs->partition_num = i;
        pthread_create(&reducer_threads[i], NULL, (void *) reduce_, (void *) reduceArgs);
    }

    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_threads[i], NULL);
    }
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




