#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>
#include <semaphore.h>

// Buffer sizes.
#define LDS_MAX_PATH 8192
#define LDS_EVENT_SIZE  ( sizeof ( struct inotify_event ) )
#define LDS_EVENT_BUFFER_LENGTH ( 1024 * ( LDS_EVENT_SIZE + 16 ) )

// Semaphore indicating all files are now being watched.
sem_t lds_init_wait;
sem_t lds_syncing;

// Base rsync command (for initial sync).
const char *lds_rsync_base = "rsync -azv --delete --fuzzy --partial";

// Source and destination paths (global).
char *lds_source_path;
char *lds_destination_path;

// Number of current notifiers.
int lds_watcher_count;
int LDS_MAX_WATCHERS;

// File info container.
typedef struct lds_file {
  char *path;
  long source_position;
  long destination_position;
} lds_file;

// List of notifiers.
typedef struct lds_notifier {
  int notifier;
  lds_file *watchers;
} lds_notifier;

// Return value from adding items.
typedef struct lds_notifier_pointer {
  lds_notifier *directory;
  lds_notifier *file;
} lds_notifier_pointer;

// Global notifier pointer.
lds_notifier_pointer notifier_pointer;

// Functions.
int lds_verify_directory( char * );
void *lds_directory_notifier( void * );
void *lds_file_notifier( void * );
int lds_notifier_thread( lds_notifier *, int );
int lds_watch( char *, int );
int lds_watch_directory( char * );
int lds_watch_file( char * );
int lds_add_items( char * );
int lds_start( );

// Escape a path.
void lds_escape_path( char *original, char escaped[LDS_MAX_PATH] ) {
  char *e_temp;

  memset( escaped, '\0', LDS_MAX_PATH );
  e_temp = escaped;
  while ( *original ) {
    if ( ( *original >= 'a' && *original <= 'z' ) ||
         ( *original >= 'A' && *original <= 'Z' ) ||
         ( *original >= '0' && *original <= '9' ) ||
         ( *original >= 45 && *original <= 47 ) ) {
      // No escape.
    } else {
      *e_temp++ = '\\';
    }
    *e_temp++ = *original++;
  }
}

// Make sure directory exists.
int lds_verify_directory( char *directory ) {
  struct stat fstat;

  // lstat() this directory.
  if ( lstat( directory, &fstat ) == 0 ) {
    if ( ! fstat.st_mode & S_IFDIR ) {
      fprintf( stderr, "%s is not a directory!\n", directory );
      return 0;
    }
  } else {
    fprintf( stderr, "No such directory: %s\n", directory );
    return 0;
  }

  // Success.
  return 1;
}

// Get path without the source directory.
char *lds_path_without_base( char *path, char *base ) {
  char *temp;
  char *source;

  temp = path;
  source = base;
  while ( *temp && *source && *temp++ == *source++ ) {
    // No-op.
  }
  if ( *temp == '/' ) {
    *temp++;
  }

  // Return result.
  return temp;
}

// Perform the initial sync.
int lds_sync_directory( char *path ) {
  int ret;
  int length;
  char command[LDS_MAX_PATH * 2];
  char source_original[LDS_MAX_PATH];
  char destination_original[LDS_MAX_PATH];
  char source[LDS_MAX_PATH];
  char destination[LDS_MAX_PATH];
  char *partial;

  // Wait for other syncs, then block.
  sem_wait( &lds_syncing );

  // Generate full source and destination directories.
  partial = lds_path_without_base( path, lds_source_path );
  sprintf( source_original, "%s/%s/", lds_source_path, partial );
  sprintf( destination_original, "%s/%s/", lds_destination_path, partial );
  lds_escape_path( source_original, source );
  lds_escape_path( destination_original, destination );

  // Generate system command.
  sprintf( command, "%s %s %s >/dev/null 2>&1", lds_rsync_base, source, destination );
  fprintf( stderr, "Command: %s\n", command );

  // Run the command.
  //if ( ( ret = system( command ) ) != 0 ) {
  //  fprintf( stderr, "Error syncing: %s\n", path );
  //  sem_post( &lds_syncing );
  //  return 0;
  //}

  // Unlock sync.
  sem_post( &lds_syncing );

  // Success.
  return 1;
}

// Remove a directory in destination path.
int lds_remove_directory( int wd, char *directory ) {
  char *path;
  char destination_original[LDS_MAX_PATH];
  char destination[LDS_MAX_PATH * 2];
  char *partial;

  // Wait for other syncs, then block.
  sem_wait( &lds_syncing );

  path = notifier_pointer.directory->watchers[wd].path;
  partial = lds_path_without_base( path, lds_source_path );
  if ( partial && *partial ) {
    sprintf( destination_original, "%s/%s/%s", lds_destination_path, partial, directory );
  } else {
    sprintf( destination_original, "%s/%s", lds_destination_path, directory );
  }
  lds_escape_path( destination_original, destination );
  fprintf( stderr, "Run: rm -rf %s\n", destination );

  // Unlock sync.
  sem_post( &lds_syncing );

  // Success.
  return 1;
}

// Remove a file in destination path.
int lds_remove_file( int wd ) {
  char *path;
  char destination_original[LDS_MAX_PATH];
  char destination[LDS_MAX_PATH * 2];

  // Wait for other syncs, then block.
  sem_wait( &lds_syncing );

  // Delete the file.
  path = notifier_pointer.file->watchers[wd].path;
  sprintf( destination_original, "%s/%s", lds_destination_path, lds_path_without_base( path, lds_source_path ) );
  lds_escape_path( destination_original, destination );
  fprintf( stderr, "Run: rm -f %s\n", destination );

  // Remove the watcher.
  inotify_rm_watch( notifier_pointer.file->notifier, wd );

  // Unlock sync.
  sem_post( &lds_syncing );

  // Success.
  return 1;
}

// Create a directory in destination path.
int lds_create_directory( char *path ) {
  return lds_sync_directory( path );
}

// Directory notifier.
void *lds_directory_notifier( void *args ) {
  int i;
  int length;
  uint32_t last_cookie;
  struct inotify_event *event;
  struct inotify_event *next;
  char buffer[LDS_EVENT_BUFFER_LENGTH];
  char temp_path[LDS_MAX_PATH];
  char next_path[LDS_MAX_PATH];
  lds_notifier *notifier;

  // Wait for the initialization lock.
  sem_wait( &lds_init_wait );
  sem_post( &lds_init_wait );

  // Infinitely wait for changes.
  notifier = notifier_pointer.directory;
  while ( 1 ) {
    // Read notification buffer.
    if ( ( length = read( notifier->notifier, buffer, LDS_EVENT_BUFFER_LENGTH ) ) < 0 ) {
      fprintf( stderr, "Error reading notifier (%d)!\n", errno );
      return;
    }

    // Parse each notification item.
    i = 0;
    last_cookie = 0;
    while ( i < length ) {
      // Clean up the path to event items.
      memset( temp_path, '\0', LDS_MAX_PATH );
      memset( next_path, '\0', LDS_MAX_PATH );

      // Get the event (and possible next event).
      event = ( struct inotify_event * )&buffer[i];
      if ( i + LDS_EVENT_SIZE + event->len < length ) {
        next = ( struct inotify_event * )&buffer[i + LDS_EVENT_SIZE + event->len];
        sprintf( next_path, "%s/%s", notifier->watchers[next->wd].path, next->name );
      } else {
        next = NULL;
      }

      // Check if this is an event that's already accounted for.
      if ( event->cookie && event->cookie == last_cookie ) {
        i += LDS_EVENT_SIZE + event->len;
        continue;
      }
      last_cookie = event->cookie;

      // Check what type of change occurred.
      if ( event->len ) {
        sprintf( temp_path, "%s/%s", notifier->watchers[event->wd].path, event->name );

        // Item was created.
        if ( event->mask & IN_CREATE ) {
          if ( event->mask & IN_ISDIR ) {
            // Directory was created, add it to watcher.
            printf( "Directory created: %s\n", temp_path );
            if ( lds_watch_directory( temp_path ) == 0 ) {
              fprintf( stderr, "Error watching new directory: %s\n", temp_path );
              i += LDS_EVENT_SIZE + event->len;
              continue;
            }
          } else {
            // File was created, add it to watcher.
            fprintf( stderr, "File created: %s\n", temp_path );
            if ( lds_watch_file( temp_path ) == 0 ) {
              fprintf( stderr, "Error watching new file: %s/%s\n", temp_path );
              i += LDS_EVENT_SIZE + event->len;
              continue;
            }
          }
        }

        // Item was deleted.
        if ( event->mask & IN_DELETE && event->mask & IN_ISDIR ) {
          // Directory was deleted.
          fprintf( stderr, "Directory deleted 1: %s\n", temp_path );
          lds_remove_directory( event->wd, event->name );
        }

        // Item was moved.
        if ( event->mask & IN_MOVED_FROM ) {
          if ( next && next->cookie == event->cookie ) {
            // Item was renamed.
            if ( event->mask & IN_ISDIR ) {
              // Directory was renamed.
              fprintf( stderr, "Directory renamed: %s => %s\n", temp_path, next_path );
            } else {
              // File was renamed.
              fprintf( stderr, "File renamed: %s => %s\n", temp_path, next_path );
            }
          } else {
            // File was moved from.
            fprintf( stderr, "File moved: %s\n", temp_path );
          }
        } else if ( event->mask & IN_MOVED_TO ) {
          if ( event->mask & IN_ISDIR ) {
            // Directory was moved to.
            fprintf( stderr, "Directory moved: %s\n", temp_path );
          } else {
            // File was moved to.
            fprintf( stderr, "File moved:%s\n", temp_path );
          }
        }
      }

      // Go to next event.
      i += LDS_EVENT_SIZE + event->len;
    }
  }
}

// File notifier.
void *lds_file_notifier( void *args ) {
  lds_notifier *notifier;
  int length;
  int i;
  struct inotify_event *event;
  char buffer[LDS_EVENT_BUFFER_LENGTH];
  char *temp_path;

  // Wait for the initialization lock.
  sem_wait( &lds_init_wait );
  sem_post( &lds_init_wait );

  // Set notifier pointer to file notifier.
  notifier = notifier_pointer.file;

  // Infinitely monitor for changes.
  while ( 1 ) {
    // Read notifications.
    if ( ( length = read( notifier->notifier, buffer, LDS_EVENT_BUFFER_LENGTH ) ) < 0 ) {
      fprintf( stderr, "Error reading notifier!\n" );
      continue;//return;
    }

    // Loop through all events.
    i = 0;
    while ( i < length ) {
      event = ( struct inotify_event * ) &buffer[ i ];
      temp_path = notifier->watchers[event->wd].path;
      if ( event->mask & IN_DELETE_SELF ) {
        // File was deleted, remove the watcher.
        fprintf( stderr, "File deleted: %s\n", temp_path );
        lds_remove_file( event->wd );
      }
      if ( event->mask & IN_OPEN ) {
        // File was opened.
        fprintf( stderr, "File opened: %s\n", temp_path );
      }
      if ( event->mask & IN_MODIFY ) {
        // File was modified.
        fprintf( stderr, "File modified: %s\n", temp_path );
      }
      if ( event->mask & IN_ACCESS ) {
        // File was accessed.
        fprintf( stderr, "File accessed: %s\n", temp_path );
      }
      if ( event->mask & IN_ATTRIB ) {
        // File attributes modified.
        fprintf( stderr, "File attribute change: %s\n", temp_path );
      }
      if ( event->mask & IN_CLOSE_WRITE ) {
        // File that was opened for writing was closed.
        fprintf( stderr, "File opened for writing closed: %s\n", temp_path );
      }
      if ( event->mask & IN_CLOSE_NOWRITE ) {
        // File that was opened for reading was closed.
        fprintf( stderr, "File opened for reading closed: %s\n", temp_path );
      }
      i += LDS_EVENT_SIZE + event->len;
    }
  }
}

// Add an item to be watched.
int lds_watch( char *path, int directory ) {
  lds_notifier *notifier;
  int watcher;

  // Make sure we have enough watchers.
  if ( lds_watcher_count >= LDS_MAX_WATCHERS ) { 
    fprintf( stderr, "Error, out of inotify watchers!\n" );
    return 0;
  }

  // Start watching.
  notifier = directory ? notifier_pointer.directory : notifier_pointer.file;
  if ( ( watcher = inotify_add_watch( notifier->notifier, path, IN_ALL_EVENTS ) ) < 0 ) {
    if ( errno == ENOSPC ) {
      fprintf( stderr, "Error, no more watchers! (stopped at: %d)\n", lds_watcher_count );
      exit( 1 );
    } else {
      fprintf( stderr, "Error watching: %s\n", path );
    }
    return 0;
  }
  lds_watcher_count++;
  notifier->watchers[watcher].path = strdup( path );
  notifier->watchers[watcher].source_position = 0;
  notifier->watchers[watcher].destination_position = 0;

  // Add items from this sub-directory.
  if ( directory ) {
    if ( lds_add_items( path ) == 0 ) {
      fprintf( stderr, "Error adding items in dir: %s\n", path );
      return 0;
    }
  }
  fprintf( stderr, "Now watching: %s\n", path );

  // Success.
  return 1;
}

// Add a directory to be watched.
int lds_watch_directory( char *path ) {
  int ret;

  ret = lds_watch( path, 1 );
  if ( ret ) {
    lds_create_directory( path );
  }
  return ret;
}

// Add a file to be watched.
int lds_watch_file( char *path ) {
  return lds_watch( path, 0 );
}

// Recursively add directories to watch.
int lds_add_items( char *path ) {
  DIR *dir;
  struct dirent *item;
  struct stat fstat;
  char temp_path[LDS_MAX_PATH];
  lds_notifier *result_notifier;

  // Open the directory.
  if ( ( dir = opendir( path ) ) == NULL ) {
    fprintf( stderr, "Error openining directory: %s\n", path );
    return 0;
  }

  // Read items from this directory.
  while ( ( item = readdir( dir ) ) != NULL ) {
    // Clean up previous path.
    memset( temp_path, '\0', LDS_MAX_PATH );

    // Ignore '.' and '..'
    if ( strcmp( item->d_name, ".." ) == 0 || strcmp( item->d_name, "." ) == 0 ) {
      continue;
    }

    // Get the full path to this item.
    sprintf( temp_path, "%s/%s", path, item->d_name );

    // Stat the item.
    if ( lstat( temp_path, &fstat ) >= 0 ) {
      // Take action based on file type.
      if ( S_ISLNK( fstat.st_mode ) || S_ISCHR( fstat.st_mode ) || S_ISBLK( fstat.st_mode ) || S_ISFIFO( fstat.st_mode ) || S_ISSOCK( fstat.st_mode ) ) {
        fprintf( stderr, "Skipping invalid file type: %s\n", temp_path );
        continue;
      } else if ( S_ISLNK( fstat.st_mode ) ) {
        // Symlink.
        continue;
      } else if ( S_ISDIR( fstat.st_mode ) ) {
        if ( lds_watch_directory( temp_path ) == 0 ) {
          return 0;
        }
      } else if ( S_ISREG( fstat.st_mode ) ) {
        if ( lds_watch_file( temp_path ) == 0 ) {
          return 0;
        }
      } else {
        // Unknown file type.
        fprintf( stderr, "Error, unknown file type: %s\n", temp_path );
        return 1;
      }
    } else {
      fprintf( stderr, "Error trying to stat: %s\n", temp_path );
      return 1;
    }
  }
  closedir( dir );

  // Success.
  return 1;
}

// Get the total possible watchers for inotify.
int lds_max_watchers( ) {
  FILE *max_watchers;
  int total;
  char buffer[LDS_EVENT_BUFFER_LENGTH];
  char *temp;

  // Read from /proc.
  if ( ( max_watchers = fopen( "/proc/sys/fs/inotify/max_user_watches", "r" ) ) == NULL ) {
    fprintf( stderr, "Error determining max_user_watches!\n" );
    return 0;
  }
  temp = buffer;
  while ( ( *temp = fgetc( max_watchers ) ) >= 0 ) {
    *temp++;
    if ( temp - buffer >= LDS_EVENT_BUFFER_LENGTH - 2 ) {
      fprintf( stderr, "max_user_watches too large to parse!\n" );
      return 0;
    }
  }
  total = 0;
  temp = buffer;
  while ( *temp && *temp >= '0' && *temp <= '9' ) {
    total = total * 10 + ( *temp - '0' );
    *temp++;
  }
  fprintf( stderr, "Max watchers available: %d\n", total );
  LDS_MAX_WATCHERS = total;

  return total;
}

// Initialize a lds instance.
int lds_start( ) {
  char *temp;
  pthread_t directory_notifier;
  pthread_t file_notifier;

  // Initialize semaphores.
  sem_init( &lds_init_wait, 0, 1 );
  sem_init( &lds_syncing, 0, 1 );
  sem_wait( &lds_init_wait );

  // Initialize notifiers pointer.
  notifier_pointer.directory = NULL;
  notifier_pointer.file = NULL;
  lds_watcher_count = 0;

  // Remove trailing /'s.
  temp = lds_source_path + strlen( lds_source_path ) - 1;
  while ( temp - lds_source_path > 0 && *temp == '/' ) {
    *temp-- = '\0';
  }
  temp = lds_destination_path + strlen( lds_destination_path ) - 1;
  while ( temp - lds_destination_path > 0 && *temp == '/' ) {
    *temp-- = '\0';
  }

  // Determine max watchers for this system.
  if ( lds_max_watchers( ) == 0 ) {
    return 0;
  }

  // Create notifier pointer.
  if ( ( notifier_pointer.directory = malloc( sizeof( lds_notifier_pointer ) ) ) == NULL ) {
    fprintf( stderr, "Error allocating memory for notifications!\n" );
    return 0;
  } else if ( ( notifier_pointer.file = malloc( sizeof( lds_notifier_pointer ) ) ) == NULL ) {
    fprintf( stderr, "Error allocating memory for notifications!\n" );
  }
  if ( ( notifier_pointer.directory->notifier = inotify_init( ) ) < 0 ) {
    fprintf( stderr, "Error initializing inotify instance!\n" );
    return 0;
  } else if ( ( notifier_pointer.file->notifier = inotify_init( ) ) < 0 ) {
    fprintf( stderr, "Error initializing inotify instance!\n" );
    return 0;
  }
  if ( ( notifier_pointer.directory->watchers = malloc( sizeof( lds_file ) * LDS_MAX_WATCHERS ) ) == NULL ) {
    fprintf( stderr, "Error allocating memory for watchers!\n" );
    return 0;
  }
  if ( ( notifier_pointer.file->watchers = malloc( sizeof( lds_file ) * LDS_MAX_WATCHERS ) ) == NULL ) {
    fprintf( stderr, "Error allocating memory for watchers!\n" );
    return 0;
  }

  // Create the directory and file notifier threads.
  pthread_create( &directory_notifier, NULL, lds_directory_notifier, ( void * )notifier_pointer.directory );
  pthread_detach( directory_notifier );
  pthread_create( &file_notifier, NULL, lds_file_notifier, ( void * )notifier_pointer.file );
  pthread_detach( file_notifier );

  // Start watching.
  if ( lds_watch_directory( lds_source_path ) == 0 ) {
    return 0;
  }

  // Unlock the semaphore.
  sem_post( &lds_init_wait );

  // Make sure threads are alive.
  while ( 1 ) {
    if ( pthread_kill( directory_notifier, 0 ) == ESRCH ) {
      fprintf( stderr, "Directory notifier died!\n" );
      return 0;
    }
    if ( pthread_kill( file_notifier, 0 ) == ESRCH ) {
      fprintf( stderr, "File notifier died!\n" );
      return 0;
    }
    sleep( 1 );
  }
}

// Main.
int main( int argc, char *argv[] ) {
  int i;
  char buffer[LDS_EVENT_BUFFER_LENGTH];
  int length;

  // Validate args.
  if ( argc != 3 ) {
    printf( "Invalid arguments, use: %s <source directory> <destination directory>\n", argv[0] );
    return 1;
  }
  lds_source_path = argv[1];
  lds_destination_path = argv[2];
  if ( lds_verify_directory( lds_source_path ) == 0 || lds_verify_directory( lds_destination_path ) == 0 ) {
    return 1;
  }

  // Initialize lds and start monitoring.
  if ( lds_start( ) == 0 ) {
    return 1;
  }
}
