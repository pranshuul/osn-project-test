#include "../include/file_locking.h"
#include "../include/common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>

// Global hash table to store file locks
static GHashTable* file_locks = NULL;
static pthread_mutex_t file_locks_mutex = PTHREAD_MUTEX_INITIALIZER;

// Initialize the file locking system
void file_locking_init() {
    if (!file_locks) {
        file_locks = g_hash_table_new_full(
            g_str_hash, g_str_equal, 
            g_free, // Key destroy function
            (GDestroyNotify)free_file_lock // Value destroy function
        );
        if (!file_locks) {
            log_message("FILE_LOCKING", "ERROR", "Failed to initialize file locks hash table");
            exit(EXIT_FAILURE);
        }
    }
}

// Clean up the file locking system
void file_locking_cleanup() {
    if (file_locks) {
        g_hash_table_destroy(file_locks);
        file_locks = NULL;
    }
    pthread_mutex_destroy(&file_locks_mutex);
}

// Helper function to free a FileLock structure
static void free_file_lock(FileLock* fl) {
    if (fl) {
        pthread_rwlock_destroy(&fl->lock);
        g_free(fl->filename);
        g_free(fl);
    }
}

// Get or create a file lock
static FileLock* get_or_create_file_lock(const char* filename) {
    FileLock* fl = NULL;
    
    pthread_mutex_lock(&file_locks_mutex);
    
    // Try to find existing lock
    fl = g_hash_table_lookup(file_locks, filename);
    
    if (!fl) {
        // Create new file lock
        fl = g_malloc0(sizeof(FileLock));
        if (fl) {
            if (pthread_rwlock_init(&fl->lock, NULL) != 0) {
                log_message("FILE_LOCKING", "ERROR", "Failed to initialize rwlock for %s: %s", 
                           filename, strerror(errno));
                g_free(fl);
                pthread_mutex_unlock(&file_locks_mutex);
                return NULL;
            }
            fl->filename = g_strdup(filename);
            fl->ref_count = 0;
            fl->last_accessed = time(NULL);
            
            g_hash_table_insert(file_locks, g_strdup(filename), fl);
        }
    }
    
    if (fl) {
        fl->ref_count++;
        fl->last_accessed = time(NULL);
    }
    
    pthread_mutex_unlock(&file_locks_mutex);
    return fl;
}

// Get a read lock for a file
int file_read_lock(const char* filename) {
    if (!filename || *filename == '\0') {
        return -1;
    }
    
    FileLock* fl = get_or_create_file_lock(filename);
    if (!fl) {
        return -1;
    }
    
    int result = pthread_rwlock_rdlock(&fl->lock);
    if (result != 0) {
        log_message("FILE_LOCKING", "ERROR", "Failed to acquire read lock for %s: %s", 
                   filename, strerror(result));
        return -1;
    }
    
    return 0;
}

// Get a write lock for a file
int file_write_lock(const char* filename) {
    if (!filename || *filename == '\0') {
        return -1;
    }
    
    FileLock* fl = get_or_create_file_lock(filename);
    if (!fl) {
        return -1;
    }
    
    int result = pthread_rwlock_wrlock(&fl->lock);
    if (result != 0) {
        log_message("FILE_LOCKING", "ERROR", "Failed to acquire write lock for %s: %s", 
                   filename, strerror(result));
        return -1;
    }
    
    return 0;
}

// Release a file lock
int file_unlock(const char* filename) {
    if (!filename || *filename == '\0') {
        return -1;
    }
    
    pthread_mutex_lock(&file_locks_mutex);
    
    FileLock* fl = g_hash_table_lookup(file_locks, filename);
    if (!fl) {
        pthread_mutex_unlock(&file_locks_mutex);
        log_message("FILE_LOCKING", "WARNING", "Attempted to unlock non-existent file: %s", filename);
        return -1;
    }
    
    int result = pthread_rwlock_unlock(&fl->lock);
    if (result != 0) {
        pthread_mutex_unlock(&file_locks_mutex);
        log_message("FILE_LOCKING", "ERROR", "Failed to unlock %s: %s", 
                   filename, strerror(result));
        return -1;
    }
    
    fl->ref_count--;
    fl->last_accessed = time(NULL);
    
    // Clean up lock if no longer in use (optional, could keep for reuse)
    if (fl->ref_count <= 0) {
        g_hash_table_remove(file_locks, filename);
    }
    
    pthread_mutex_unlock(&file_locks_mutex);
    return 0;
}

// Remove a file lock (when file is deleted)
int file_lock_remove(const char* filename) {
    if (!filename || *filename == '\0') {
        return -1;
    }
    
    pthread_mutex_lock(&file_locks_mutex);
    
    FileLock* fl = g_hash_table_lookup(file_locks, filename);
    if (fl) {
        // Wait until all references are released
        while (fl->ref_count > 0) {
            pthread_mutex_unlock(&file_locks_mutex);
            usleep(10000); // 10ms
            pthread_mutex_lock(&file_locks_mutex);
            fl = g_hash_table_lookup(file_locks, filename);
            if (!fl) {
                pthread_mutex_unlock(&file_locks_mutex);
                return 0; // Already removed by another thread
            }
        }
        
        // Remove from hash table and free
        g_hash_table_remove(file_locks, filename);
    }
    
    pthread_mutex_unlock(&file_locks_mutex);
    return 0;
}

// Dump all active locks (for debugging)
void file_lock_dump() {
    GList* keys = NULL;
    GList* iter = NULL;
    
    pthread_mutex_lock(&file_locks_mutex);
    
    keys = g_hash_table_get_keys(file_locks);
    log_message("FILE_LOCKING", "DEBUG", "Active file locks (%d):", g_list_length(keys));
    
    for (iter = keys; iter != NULL; iter = iter->next) {
        const char* filename = (const char*)iter->data;
        FileLock* fl = g_hash_table_lookup(file_locks, filename);
        if (fl) {
            char time_buf[64];
            strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", localtime(&fl->last_accessed));
            log_message("FILE_LOCKING", "DEBUG", "  %s (refs: %d, last: %s)", 
                       filename, fl->ref_count, time_buf);
        }
    }
    
    if (keys) {
        g_list_free(keys);
    }
    
    pthread_mutex_unlock(&file_locks_mutex);
}

// Check if a file is currently locked
bool file_is_locked(const char* filename) {
    if (!filename || *filename == '\0') {
        return false;
    }
    
    bool is_locked = false;
    
    pthread_mutex_lock(&file_locks_mutex);
    
    FileLock* fl = g_hash_table_lookup(file_locks, filename);
    if (fl) {
        // Try to acquire a read lock to check if it's locked
        int result = pthread_rwlock_tryrdlock(&fl->lock);
        if (result == 0) {
            // Got the lock, so it wasn't locked
            pthread_rwlock_unlock(&fl->lock);
            is_locked = false;
        } else if (result == EBUSY) {
            // Lock is held by a writer or other readers
            is_locked = true;
        }
    }
    
    pthread_mutex_unlock(&file_locks_mutex);
    return is_locked;
}
