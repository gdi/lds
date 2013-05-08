#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>

// Event size and event buffer length.
#define LDS_EVENT_SIZE  ( sizeof ( struct inotify_event ) )
#define LDS_EVENT_BUFFER_LENGTH ( 1024 * ( LDS_EVENT_SIZE + 16 ) )

// Types of events to listen to.
#define LDS_DIR_EVENT_TYPES IN_ATTRIB|IN_CREATE|IN_DELETE|IN_DELETE_SELF|IN_MOVE_SELF|IN_MOVED_FROM|IN_MOVED_TO
#define LDS_FILE_EVENT_TYPES IN_ACCESS|IN_ATTRIB|IN_CLOSE_WRITE|IN_CLOSE_NOWRITE|IN_MODIFY

// Base rsync command (for initial sync).
const char *lds_rsync_base = "rsync -azv --delete --fuzzy --partial";

// Source and destination paths (global).
char *source_path;
char *destination_path;

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

// Directory notifier.
void *lds_directory_notifier( void *args ) {
  int i;
  int length;
  struct inotify_event *event;
  char buffer[LDS_EVENT_BUFFER_LENGTH];
  char *temp_path;
  lds_notifier *notifier;

  // Infinitely wait for changes.
  notifier = notifier_pointer.directory;
  while ( 1 ) {
    // Read notification buffer.
    if ( ( length = read( notifier->notifier, buffer, LDS_EVENT_BUFFER_LENGTH ) ) < 0 ) {
      fprintf( stderr, "Error reading notifier!\n" );
      return;
    }

    // Parse each notification item.
    i = 0;
    temp_path = NULL;
    while ( i < length ) {
      event = ( struct inotify_event * ) &buffer[ i ];

      // Clean up the path to event item.
      if ( temp_path ) {
        free( temp_path );
        temp_path = NULL;
      }

      // Check what type of change occurred.
      if ( event->len ) {
        if ( ( temp_path = malloc( strlen( event->name ) + strlen( notifier->watchers[event->wd].path ) + 2 ) ) == NULL ) {
          sprintf( "Error watching new item: %s/%s\n", notifier->watchers[event->wd].path, event->name );
          i += LDS_EVENT_SIZE + event->len;
          continue;
        }
        sprintf( temp_path, "%s/%s", notifier->watchers[event->wd].path, event->name );
        if ( event->mask & IN_CREATE ) {
          if ( event->mask & IN_ISDIR ) {
            printf( "New directory %s created.\n", event->name );
            if ( lds_watch_directory( temp_path ) == 0 ) {
              fprintf( stderr, "Error watching new directory: %s\n", temp_path );
              i += LDS_EVENT_SIZE + event->len;
              continue;
            }
          } else {
            if ( lds_watch_file( temp_path ) == 0 ) {
              fprintf( stderr, "Error watching new file: %s/%s\n", temp_path );
              i += LDS_EVENT_SIZE + event->len;
              continue;
            }
          }
        } else if ( event->mask & IN_DELETE ) {
          if ( event->mask & IN_ISDIR ) {
            printf( "Directory %s deleted.\n", event->name );
          } else {
            printf( "File %s deleted (in %s).\n", event->name, notifier->watchers[event->wd].path );
          }
        } else if ( event->mask & IN_MOVED_FROM || event->mask & IN_MOVED_TO ) {
          if ( event->mask & IN_ISDIR ) {
            printf( "Directory %s moved.\n", event->name );
          } else {
            printf( "File %s moved.\n", event->name );
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

  // Set notifier pointer to file notifier.
  notifier = notifier_pointer.file;

  // Infinitely monitor for changes.
  while ( 1 ) {
    // Read notifications.
    if ( ( length = read( notifier->notifier, buffer, LDS_EVENT_BUFFER_LENGTH ) ) < 0 ) {
      fprintf( stderr, "Error reading notifier!\n" );
      return;
    }

    // Loop through all events.
    i = 0;
    while ( i < length ) {
      event = ( struct inotify_event * ) &buffer[ i ];
      if ( event->mask & IN_MODIFY ) {
        fprintf( stderr, "File modified: %s\n", notifier->watchers[event->wd].path );
      }
      i += LDS_EVENT_SIZE + event->len;
    }
  }
}

// Add an item to be watched.
int lds_watch( char *path, int directory ) {
  uint32_t flags;
  lds_notifier *notifier;
  int watcher;

  // Make sure we have enough watchers.
  if ( lds_watcher_count >= LDS_MAX_WATCHERS ) { 
    fprintf( stderr, "Error, out of inotify watchers!\n" );
    return 0;
  }

  // Start watching.
  flags = 0;
  if ( directory ) {
    notifier = notifier_pointer.directory;
    flags = LDS_DIR_EVENT_TYPES;
  } else {
    notifier = notifier_pointer.file;
    flags = LDS_FILE_EVENT_TYPES;
  }
  if ( ( watcher = inotify_add_watch( notifier->notifier, path, flags ) ) < 0 ) {
    fprintf( stderr, "Error watching: %s\n", path );
    return 0;
  }
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
  return lds_watch( path, 1 );
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
  char *temp_path;
  lds_notifier *result_notifier;

  // Open the directory.
  if ( ( dir = opendir( path ) ) == NULL ) {
    fprintf( stderr, "Error openining directory: %s\n", path );
    return 0;
  }

  // Read items from this directory.
  temp_path = NULL;
  while ( ( item = readdir( dir ) ) != NULL ) {
    // Clean up previous path.
    if ( temp_path ) {
      free( temp_path );
      temp_path = NULL;
    }

    // Ignore '.' and '..'
    if ( strcmp( item->d_name, ".." ) == 0 || strcmp( item->d_name, "." ) == 0 ) {
      continue;
    }

    // Get the full path to this item.
    if ( ( temp_path = malloc( strlen( path ) + strlen( item->d_name ) + 3 ) ) == NULL ) {
      fprintf( stderr, "Error adding item to watch: %s/%s\n", path, item->d_name );
      return 0;
    }
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

// Perform the initial sync.
int lds_initial_sync( ) {
  int ret;
  int length;
  char *command;

  // Generate system command.
  length = strlen( lds_rsync_base ) + strlen( source_path ) + strlen( destination_path ) + 22;
  if ( ( command = malloc( length ) ) == NULL ) {
    fprintf( stderr, "Error performing initial sync!\n" );
    return 0;
  }
  sprintf( command, "%s %s/ %s/ > /dev/null 2>&1", lds_rsync_base, source_path, destination_path );
  if ( ( ret = system( command ) ) != 0 ) {
    fprintf( stderr, "Error performing initial sync!\n" );
    return 0;
  }

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
}

// Initialize a lds instance.
int lds_start( ) {
  char *temp;
  pthread_t directory_notifier;
  pthread_t file_notifier;

  // Initialize notifiers pointer.
  notifier_pointer.directory = NULL;
  notifier_pointer.file = NULL;
  lds_watcher_count = 0;

  // Remove trailing /'s.
  temp = source_path + strlen( source_path ) - 1;
  while ( temp - source_path > 0 && *temp == '/' ) {
    *temp-- = '\0';
  }
  temp = destination_path + strlen( destination_path ) - 1;
  while ( temp - destination_path > 0 && *temp == '/' ) {
    *temp-- = '\0';
  }

  // Determine max watchers for this system.
  if ( lds_max_watchers( ) == 0 ) {
    return 0;
  }

  // Perform initial sync.
  if ( lds_initial_sync( ) == 0 ) {
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
  if ( lds_watch_directory( source_path ) == 0 ) {
    return 0;
  }

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
  source_path = argv[1];
  destination_path = argv[2];
  if ( lds_verify_directory( source_path ) == 0 || lds_verify_directory( destination_path ) == 0 ) {
    return 1;
  }

  // Initialize lds and start monitoring.
  if ( lds_start( ) == 0 ) {
    return 1;
  }
}
