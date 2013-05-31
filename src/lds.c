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
const char *lds_rsync_base = "rsync -azv --delete --fuzzy --partial --inplace";
const char *lds_rsync_append = "rsync -azv --append --inplace";
const char *lds_rsync_partial = "rsync -azv --partial --inplace";

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
  int fd;
  lds_file *watchers;
} lds_notifier;

// Global notifier pointer.
lds_notifier *notifier;

// Functions.
int lds_verify_directory( char * );
int lds_notifier_thread( lds_notifier *, int );
int lds_watch( char *, int );
int lds_watch_directory( char * );
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
         ( *original >= 45 && *original <= 47 ) ||
         ( *original == '_' ) ) {
      // No escape.
    } else {
      *e_temp++ = '\\';
    }
    *e_temp++ = *original++;
  }
}

// Create the lds directory in destination.
int lds_create_destination_directory( ) {
  char *path;
  struct stat fstat;

  // lstat() the directory to see if it exists.
  if ( ( path = malloc( strlen( lds_destination_path ) + strlen( "/lds-data" ) + 1 ) ) == NULL ) {
    fprintf( stderr, "Error storing destination path!\n" );
    return 0;
  }
  sprintf( path, "%s/lds-data", lds_destination_path );
  if ( lstat( path, &fstat ) == 0 ) {
    if ( ! fstat.st_mode & S_IFDIR ) {
      fprintf( stderr, "%s is not a directory!\n", path );
      return 0;
    }
  } else {
    // Create the directory.
    if ( mkdir( path, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH ) < 0 ) {
      fprintf( stderr, "Error creating lds directory: %s\n", path );
      return 0;
    }
  }

  // Change destination path.
  lds_destination_path = path;

  // Success.
  return 1;
}

// Make sure directory exists.
int lds_verify_directory( char *directory ) {
  char *temp;
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

  // Remove trailing /'s.
  temp = directory + strlen( directory ) - 1;
  while ( temp - directory > 0 && *temp == '/' ) {
    *temp-- = '\0';
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
  char command[LDS_MAX_PATH * 3];
  char source_original[LDS_MAX_PATH];
  char destination_original[LDS_MAX_PATH];
  char source[LDS_MAX_PATH];
  char destination[LDS_MAX_PATH];
  char *partial;

  // Wait for other syncs, then block.
  sem_wait( &lds_syncing );

  // Generate full source and destination directories.
  partial = lds_path_without_base( path, lds_source_path );
  if ( partial && *partial ) {
    sprintf( source_original, "%s/%s/", lds_source_path, partial );
    sprintf( destination_original, "%s/%s/", lds_destination_path, partial );
  } else {
    sprintf( source_original, "%s/", lds_source_path );
    sprintf( destination_original, "%s/", lds_destination_path );
  }
  lds_escape_path( source_original, source );
  lds_escape_path( destination_original, destination );

  // Generate system command.
  sprintf( command, "%s %s %s > /dev/null 2>&1", lds_rsync_base, source, destination );
  fprintf( stderr, "Command: %s\n", command );

  // Run the command.
  if ( ( ret = system( command ) ) != 0 ) {
    fprintf( stderr, "Error syncing: %s\n", path );
    sem_post( &lds_syncing );
    return 0;
  }

  // Unlock sync.
  sem_post( &lds_syncing );

  // Success.
  return 1;
}

// Recursively delete an item.
int lds_recursive_delete( char *path ) {
  DIR *dir;
  struct dirent *item;
  char target[LDS_MAX_PATH];
  struct stat fstat;

  // Stat the item.
  if ( lstat( path, &fstat ) < 0 ) {
    fprintf( stderr, "Error trying stat: %s\n", path );
    return 0;
  }

  if ( S_ISDIR( fstat.st_mode ) ) {
    // Read directory and delete files.
    if ( ( dir = opendir( path ) ) == NULL ) {
      fprintf( stderr, "Error openining directory: %s\n", path );
      return 0;
    }

    // Read items from this directory.
    while ( ( item = readdir( dir ) ) != NULL ) {
      // Ignore '.' and '..'
      if ( strcmp( item->d_name, ".." ) == 0 || strcmp( item->d_name, "." ) == 0 ) {
        continue;
      }

      // Delete this item.
      memset( target, '\0', LDS_MAX_PATH );
      sprintf( target, "%s/%s", path, item->d_name );
      if ( lds_recursive_delete( path ) == 0 ) {
        return 0;
      }
    }

    // Close the directory and delete it.
    closedir( dir );
    if ( rmdir( path ) < 0 ) {
      return 0;
    }
  } else {
    // Normal file, delete it.
    if ( unlink( path ) < 0 ) {
      fprintf( stderr, "Error deleting: %s\n", path );
      return 0;
    }
  }

  // Success.
  return 1;
}

// Remove a directory in destination path.
int lds_remove_item( int wd, char *name ) {
  char *path;
  char destination[LDS_MAX_PATH];
  char *partial;

  // Wait for other syncs, then block.
  sem_wait( &lds_syncing );

  // Get the paths.
  path = notifier->watchers[wd].path;
  partial = lds_path_without_base( path, lds_source_path );
  if ( partial && *partial ) {
    sprintf( destination, "%s/%s/%s", lds_destination_path, partial, name );
  } else {
    sprintf( destination, "%s/%s", lds_destination_path, name );
  }

  // Recursively delete.
  if ( lds_recursive_delete( destination ) == 0 ) {
    fprintf( stderr, "Unable to delete: %s\n", destination );
  }

  // Unlock sync.
  sem_post( &lds_syncing );

  // Success.
  return 1;
}

// Create a file in destination path.
int lds_sync_file( char *path, int modify ) {
  char destination[LDS_MAX_PATH];
  char e_path[LDS_MAX_PATH * 2];
  char e_destination[LDS_MAX_PATH * 2];
  char command[LDS_MAX_PATH * 3];
  char *partial;
  int ret;

  // Wait for other syncs, then block.
  sem_wait( &lds_syncing );

  // Generate the rsync command.
  partial = lds_path_without_base( path, lds_source_path );
  if ( partial && *partial ) {
    sprintf( destination, "%s/%s", lds_destination_path, partial );
  }
  lds_escape_path( destination, e_destination );
  lds_escape_path( path, e_path );
  if ( modify == 1 ) {
    sprintf( command, "%s %s %s > /dev/null 2>&1", lds_rsync_append, e_path, e_destination );
  } else {
    sprintf( command, "%s %s %s > /dev/null 2>&1", lds_rsync_partial, e_path, e_destination );
  }
  fprintf( stderr, "Sync file: %s\n", command );

  // Run the command.
  if ( ( ret = system( command ) ) != 0 ) {
    fprintf( stderr, "Error syncing: %s\n", path );
    sem_post( &lds_syncing );
    return 0;
  }

  // Unlock sync.
  sem_post( &lds_syncing );

  // Success.
  return 1;
}

// Rename a file.
int lds_rename_item( char *original, char *new_name ) {
  char *o_partial;
  char *n_partial;
  char d_source[LDS_MAX_PATH];
  char d_destination[LDS_MAX_PATH];

  // Get paths.
  o_partial = lds_path_without_base( original, lds_source_path );
  n_partial = lds_path_without_base( new_name, lds_source_path );
  sprintf( d_source, "%s/%s", lds_destination_path, o_partial );
  sprintf( d_destination, "%s/%s", lds_destination_path, n_partial );

  // Rename the item.
  if ( rename( d_source, d_destination ) != 0 ) {
    fprintf( stderr, "Error renaming: %s => %s\n", d_source, d_destination );
    return 0;
  }

  // Success.
  return 1;
}

// Directory notifier.
void *lds_notifier_worker( void * args ) {
  int i;
  int length;
  struct stat fstat;
  uint32_t last_cookie;
  struct inotify_event *event;
  struct inotify_event *next;
  char buffer[LDS_EVENT_BUFFER_LENGTH];
  char temp_path[LDS_MAX_PATH];
  char next_path[LDS_MAX_PATH];

  // Wait for the initialization lock.
  sem_wait( &lds_init_wait );
  sem_post( &lds_init_wait );

  // Perform the initial sync.
  if ( lds_sync_directory( lds_source_path ) == 0 ) {
    fprintf( stderr, "Error performing initial sync!\n" );
    exit( 1 );
  }

  // Infinitely wait for changes.
  while ( 1 ) {
    // Read notification buffer.
    if ( ( length = read( notifier->fd, buffer, LDS_EVENT_BUFFER_LENGTH ) ) < 0 ) {
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

        if ( event->mask & IN_CREATE ) {
          // Creations.
          if ( event->mask & IN_ISDIR ) {
            // Directory was created, add it to watcher.
            printf( "Directory created: %s\n", temp_path );
            if ( lds_watch_directory( temp_path ) == 0 ) {
              fprintf( stderr, "Error watching new directory: %s\n", temp_path );
              i += LDS_EVENT_SIZE + event->len;
              continue;
            } else if ( lds_sync_directory( temp_path ) == 0 ) {
              i += LDS_EVENT_SIZE + event->len;
            }
          } else {
            // File was created, add it to watcher.
            fprintf( stderr, "File created: %s\n", temp_path );
            if ( lds_sync_file( temp_path, 0 ) == 0 ) {
              i += LDS_EVENT_SIZE + event->len;
            }
          }
        } else if ( event->mask & IN_DELETE ) {
          // Deletions.
          if ( event->mask & IN_ISDIR ) {
            // Directory was deleted.
            fprintf( stderr, "Directory deleted: %s\n", temp_path );
            lds_remove_item( event->wd, event->name );
          } else {
            // File was deleted.
            fprintf( stderr, "File deleted: %s\n", temp_path );
            lds_remove_item( event->wd, event->name );
          }
        } else if ( event->mask & IN_MOVED_FROM ) {
          // Moves/renames.
          if ( next && next->cookie == event->cookie ) {
            // Item was renamed.
            fprintf( stderr, "Rename: %s => %s\n", temp_path, next_path );
            lds_rename_item( temp_path, next_path );
            if ( event->mask & IN_ISDIR ) {
              // Perform sync of this directory.
              lds_sync_directory( next_path );
            } else {
              // Perform sync of this new file.
              lds_sync_file( next_path, 0 );
            }
          } else {
            if ( event->mask & IN_ISDIR ) {
              // Directory was moved from an external directory into this directory.
              fprintf( stderr, "Directory moved into watched dir: %s\n", temp_path );
              if ( lds_watch_directory( temp_path ) == 0 ) {
                fprintf( stderr, "Error watching directory: %s\n", temp_path );
              }
            } else {
              // File was moved from an external directory into this directory.
              fprintf( stderr, "File moved into watched dir: %s\n", temp_path );
              lds_sync_file( temp_path, 0 );
            }
          }
        } else if ( event->mask & IN_MOVED_TO && event->cookie == 0 ) {
          if ( event->mask & IN_ISDIR ) {
            // Directory was moved to.
            fprintf( stderr, "Directory moved: %s\n", temp_path );
            lds_remove_item( event->wd, event->name );
          } else {
            // File was moved to.
            fprintf( stderr, "File moved:%s\n", temp_path );
            lds_remove_item( event->wd, event->name );
          }
        } else if ( event->mask & IN_MODIFY || event->mask & IN_ATTRIB ) {
          // Check if it's a file.
          if ( lstat( temp_path, &fstat ) < 0 ) {
            i += LDS_EVENT_SIZE + event->len;
            continue;
          }

          // Check the type of file.
          if ( S_ISREG( fstat.st_mode ) == 0 ) {
            i += LDS_EVENT_SIZE + event->len;
            continue;
          }

          if ( event->mask & IN_MODIFY ) {
            // File was modified.
            fprintf( stderr, "File modified: %s\n", temp_path );
            if ( lds_sync_file( temp_path, 1 ) == 0 ) {
              i += LDS_EVENT_SIZE + event->len;
              continue;
            }
          }

          if ( event->mask & IN_ATTRIB ) {
            // Attributes changed.
            fprintf( stderr, "Attributes changed: %s\n", temp_path );
          }
        }
      }

      // Go to next event.
      i += LDS_EVENT_SIZE + event->len;
    }
  }
}

// Add an item to be watched.
int lds_watch_directory( char *path ) {
  int watcher;

  // Make sure we have enough watchers.
  if ( lds_watcher_count >= LDS_MAX_WATCHERS ) { 
    fprintf( stderr, "Error, out of inotify watchers!\n" );
    return 0;
  }

  // Start watching.
  if ( ( watcher = inotify_add_watch( notifier->fd, path, IN_ALL_EVENTS ) ) < 0 ) {
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
  if ( lds_add_items( path ) == 0 ) {
    fprintf( stderr, "Error adding items in dir: %s\n", path );
    return 0;
  }
  fprintf( stderr, "Now watching: %s\n", path );

  // Success.
  return 1;
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
        // Invalid file type.
        continue;
      } else if ( S_ISLNK( fstat.st_mode ) ) {
        // Symlink.
        continue;
      } else if ( S_ISDIR( fstat.st_mode ) ) {
        // Directory.
        if ( lds_watch_directory( temp_path ) == 0 ) {
          return 0;
        }
      } else if ( S_ISREG( fstat.st_mode ) ) {
        // Regular file.
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
  pthread_t runner;

  // Initialize semaphores.
  sem_init( &lds_init_wait, 0, 1 );
  sem_init( &lds_syncing, 0, 1 );
  sem_wait( &lds_init_wait );

  // Initialize notifiers pointer.
  notifier = NULL;
  lds_watcher_count = 0;

  // Determine max watchers for this system.
  if ( lds_max_watchers( ) == 0 ) {
    return 0;
  }

  // Create notifier pointer.
  if ( ( notifier = malloc( sizeof( lds_notifier ) ) ) == NULL ) {
    fprintf( stderr, "Error allocating memory for notifications!\n" );
    return 0;
  }
  if ( ( notifier->fd = inotify_init( ) ) < 0 ) {
    fprintf( stderr, "Error initializing inotify instance!\n" );
    return 0;
  }
  if ( ( notifier->watchers = malloc( sizeof( lds_file ) * LDS_MAX_WATCHERS ) ) == NULL ) {
    fprintf( stderr, "Error allocating memory for watchers!\n" );
    return 0;
  }

  // Create the watcher thread.
  pthread_create( &runner, NULL, lds_notifier_worker, NULL );
  pthread_detach( runner );

  // Start watching.
  if ( lds_watch_directory( lds_source_path ) == 0 ) {
    return 0;
  }

  // Unlock the semaphore.
  sem_post( &lds_init_wait );

  // Make sure threads are alive.
  while ( 1 ) {
    if ( pthread_kill( runner, 0 ) == ESRCH ) {
      fprintf( stderr, "Notifier died!\n" );
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
  if ( lds_verify_directory( lds_source_path ) == 0 || lds_verify_directory( lds_destination_path ) == 0 || lds_create_destination_directory( ) == 0 ) {
    return 1;
  }

  // Initialize lds and start monitoring.
  if ( lds_start( ) == 0 ) {
    return 1;
  }
}
