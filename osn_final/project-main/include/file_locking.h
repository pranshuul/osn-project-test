#ifndef FILE_LOCKING_H
#define FILE_LOCKING_H

#include <pthread.h>
#include <glib.h>
#include <stdbool.h>

// Structure to hold file lock information
typedef struct {
    pthread_rwlock_t lock;     // Read-write lock for the file
    int ref_count;            // Reference count
    time_t last_accessed;     // Last access time
    char* filename;           // Name of the file
} FileLock;

// Initialize the file locking system
void file_locking_init();

// Clean up the file locking system
void file_locking_cleanup();

// Get a read lock for a file
int file_read_lock(const char* filename);

// Get a write lock for a file
int file_write_lock(const char* filename);

// Release a file lock
int file_unlock(const char* filename);

// Remove a file lock (when file is deleted)
int file_lock_remove(const char* filename);

// Dump all active locks (for debugging)
void file_lock_dump();

// Check if a file is currently locked
bool file_is_locked(const char* filename);

#endif // FILE_LOCKING_H
