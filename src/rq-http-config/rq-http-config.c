//-----------------------------------------------------------------------------
// rq-http-config
//	Service that accepts Http connections and passes control to particular
//	queues.
//-----------------------------------------------------------------------------


#include <rq-http-config.h>


// includes
#include <assert.h>
#include <event.h>
#include <expbuf.h>
#include <linklist.h>
#include <risp.h>
#include <rq.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define PACKAGE						"rq-http-config"
#define VERSION						"1.0"


#if (RQ_HTTP_CONFIG_VERSION != 0x00010100)
	#error "This version designed only for v1.01.00 of librq-http-config"
#endif


#if (LIBLINKLIST_VERSION < 0x00006000)
	#error "liblinklist v0.6 or higher is required"
#endif



typedef struct {
	// running params
	short verbose;
	short daemonize;
	char *username;
	char *pid_file;

	// connections to the controllers.
	list_t *controllers;

	// unique settings.
	char *configfile;
	char *queue;
} settings_t;



typedef struct {
	list_t *hosts;
	list_t *aliases;
} config_t;


typedef struct {
	char *path;
	int length;
	char *consumer;
} config_path_t;

typedef struct {
	int host_id;
	char *consumer;
	list_t *paths;
} config_host_t;

typedef struct {
	char *alias;
	int length;						// used to quickly compare the length before bothering to compare strings.
	config_host_t *host;
} config_alias_t;

typedef struct {
	config_t *config;
	config_host_t *host;
} config_combo_t;






typedef struct {
	struct event_base *evbase;
	rq_t              *rq;
	risp_t						*risp;
	settings_t        *settings;
	config_t          *config;
	expbuf_pool_t     *bufpool;

	struct event *sigint_event;
	struct event *sighup_event;

	rq_message_t *req;
	expbuf_t *reply;

	char *host;
	int host_len;
	char *path;
	int path_len;

} control_t;




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-f <filename>       Sqlite3 config file.\n");
	printf("\n");
	printf("-c <ip:port>        Controller to connect to.\n");
	printf("\n");
	printf("-d                  run as a daemon\n");
	printf("-P <file>           save PID in <file>, only used with -d option\n");
	printf("-u <username>       assume identity of <username> (only when run as root)\n");
	printf("\n");
	printf("-v                  verbose (print errors/warnings while in event loop)\n");
	printf("-h                  print this help and exit\n");
	return;
}



// Initialise the 
void config_init(config_t *config)
{
	assert(config);
	config->hosts = NULL;
	config->aliases = NULL;
}

static void config_unload(config_t *config)
{
	config_host_t *host;
	config_alias_t *alias;
	config_path_t *path;
	
	assert(config);
		


	if (config->aliases) {
		while ((alias = ll_pop_head(config->aliases))) {
			assert(alias->alias);
			free(alias->alias);
			alias->alias = NULL;
			alias->host = NULL;
			free(alias);
		}
		ll_free(config->aliases);
		free(config->aliases);
		config->aliases = NULL;
	}

		
	if (config->hosts) {
		while ((host = ll_pop_head(config->hosts))) {

			assert(host->host_id > 0);
			
			if (host->consumer) {
				free(host->consumer);
				host->consumer = NULL;
			}
			
			if (host->paths) {
				while ((path = ll_pop_head(host->paths))) {
					assert(path->length > 0);

					assert(path->path);
					free(path->path);
					path->path = NULL;

					assert(path->consumer);
					free(path->consumer);
					path->consumer = NULL;

					free(path);
				}
				ll_free(host->paths);
				free(host->paths);
				host->paths = NULL;
			}
			
			free(host);
		}
		ll_free(config->hosts);
		free(config->hosts);
		config->hosts = NULL;
	}
}


void config_free(config_t *config)
{
	
	assert(config);
	if (config->hosts || config->aliases) {
		config_unload(config);
	}
}




static int host_callback(void *ptr, int argc, char **argv, char **cols)
{
  config_t *config = ptr;
  config_host_t *host;

	assert(config);
	assert(argc > 0);
	assert(argv && cols);

	assert(argc == 2);
	assert(strcmp(cols[0], "HostID") == 0);
	assert(strcmp(cols[1], "Consumer") == 0);

  
  host = (config_host_t *) malloc(sizeof(config_host_t));
	host->host_id = atoi(argv[0]);
	host->consumer = argv[1] ? strdup(argv[1]) : NULL;
	host->paths = NULL;

	assert(config->hosts);
	ll_push_head(config->hosts, host);
	
  return 0;
}



int config_load_hosts(config_t *config, sqlite3 *dbh)
{
	int loop;
	int rc;
	char *errmsg;
	
	assert(config);
	assert(dbh);

	assert(config->aliases == NULL);
	
	assert(config->hosts == NULL);
	config->hosts = (list_t *) malloc(sizeof(list_t));
	ll_init(config->hosts);

	loop = 1;
	while (loop != 0) {
		errmsg = NULL;
		rc = sqlite3_exec(dbh, "SELECT HostID, Consumer FROM Hosts ORDER BY HostID", host_callback, config, &errmsg);
		if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
			
			// indicate that we dont want to loop any more.
			loop = 0;
			
			if (rc != SQLITE_OK) {
				fprintf(stderr, "SQL error: %s\n", errmsg);
				sqlite3_free(errmsg);
				errmsg = NULL;
				assert(ll_count(config->hosts) == 0);
			}
		}
		assert(errmsg == NULL);
	}

	// at this point we should have host entries.
	if (ll_count(config->hosts) == 0) {
		fprintf(stderr, "config does not contain any hosts\n");

		ll_free(config->hosts);
		free(config->hosts);
		config->hosts = NULL;
		
		assert(config->aliases == NULL);

		return -1;
	}
	else {
		assert(config->aliases == NULL);
		return 0;
	}
}


static int alias_callback(void *ptr, int argc, char **argv, char **cols)
{
  config_combo_t *combo = ptr;
  config_alias_t *alias;

	assert(combo);
	assert(combo->config);
	assert(combo->host);
	assert(argc > 0);
	assert(argv && cols);

	assert(argc == 1);
	assert(strcmp(cols[0], "Alias") == 0);

  alias = (config_alias_t *) malloc(sizeof(config_alias_t));
	assert(argv[0]);
	alias->alias = strdup(argv[0]);
	assert(alias->alias);
	alias->length = strlen(alias->alias);
	assert(alias->length > 0);
	alias->host = combo->host;

	assert(combo->config->aliases);
	ll_push_head(combo->config->aliases, alias);

  return 0;
}




// load the aliases.  It will do a query per host entry.
#define QUERY_LEN 1024
static int config_load_aliases(config_t *config, sqlite3 *dbh)
{
	int rc;
	config_host_t *host;
	void *next;
	unsigned char loop;
	char *errmsg;
	char query[QUERY_LEN];
	config_combo_t combo;

	assert(config);
	assert(dbh);
	
	assert(config->aliases == NULL);
	config->aliases = (list_t *) malloc(sizeof(list_t));
	ll_init(config->aliases);

	assert(config->hosts);
	next = ll_start(config->hosts);
	while ((host = ll_next(config->hosts, &next))) {

		assert(host->host_id > 0);
		snprintf(query, QUERY_LEN, "SELECT Alias FROM Aliases WHERE HostID=%d ORDER BY AliasID", host->host_id);

		combo.config = config;
		combo.host = host;		
	
		loop = 1;
		while (loop != 0) {
			errmsg = NULL;
			
			rc = sqlite3_exec(dbh, query, alias_callback, &combo, &errmsg);
			if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
				loop = 0;

				if (rc != SQLITE_OK) {
					fprintf(stderr, "SQL error: %s\n", errmsg);
					sqlite3_free(errmsg);
					errmsg = NULL;
					assert(ll_count(config->hosts) == 0);
				}
			}
			assert(errmsg == NULL);
		}
	}

	return 0;
}
#undef QUERY_LEN





static int paths_callback(void *ptr, int argc, char **argv, char **cols)
{
  config_host_t *host = ptr;
  config_path_t *path;

	assert(argc > 0);
	assert(argv && cols);

	assert(argc == 2);
	assert(strcmp(cols[0], "Path") == 0);
	assert(strcmp(cols[1], "Consumer") == 0);

  path = (config_path_t *) malloc(sizeof(config_path_t));
  assert(path);
	assert(argv[0]);
	path->path = strdup(argv[0]);
	path->length = strlen(path->path);

	// there should always be a consumer for a path.
	assert(argv[1]);
	path->consumer = strdup(argv[1]);
	assert(path->consumer);

	assert(host);
	assert(host->paths);
	ll_push_head(host->paths, path);
	
  return 0;
}




// load the aliases.  It will do a query per host entry.
#define QUERY_LEN 1024
static int config_load_paths(config_t *config, sqlite3 *dbh)
{
	int rc;
	config_host_t *host;
	void *next;
	unsigned char loop;
	char *errmsg;
	char query[QUERY_LEN];

	assert(config);
	assert(dbh);
	
	assert(config->hosts);
	next = ll_start(config->hosts);
	while ((host = ll_next(config->hosts, &next))) {

		assert(host->host_id > 0);
		snprintf(query, QUERY_LEN, "SELECT Path, Consumer FROM Paths WHERE HostID=%d ORDER BY Path", host->host_id);

		assert(host->paths == NULL);
		host->paths = (list_t *) malloc(sizeof(list_t));
		ll_init(host->paths);

		loop = 1;
		while (loop != 0) {
			errmsg = NULL;
			
			rc = sqlite3_exec(dbh, query, paths_callback, host, &errmsg);
			if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
				loop = 0;

				if (rc != SQLITE_OK) {
					fprintf(stderr, "SQL error: %s\n", errmsg);
					sqlite3_free(errmsg);
					errmsg = NULL;
					assert(ll_count(config->hosts) == 0);
				}
			}
			assert(errmsg == NULL);
		}
	}

	return 0;
}
#undef QUERY_LEN




static int config_load(config_t *config, const char *configfile)
{
	int rc;
	sqlite3 *dbh;
	
	assert(config);
	assert(configfile);

	assert(config->hosts == NULL);
	
	dbh = NULL;
	rc = sqlite3_open(configfile, &dbh);
	assert(dbh);
	if (rc != SQLITE_OK) {
		sqlite3_close(dbh);
		return(-1);
	}
	else {
		// We are connected to the config database ok.
		assert(rc == SQLITE_OK);

		// get the list of hosts from the database.
		if (config_load_hosts(config, dbh) < 0) {
			// error
			sqlite3_close(dbh);
			assert(config->hosts == NULL);
			return -1;
		}
		else {
			if (config_load_aliases(config, dbh) < 0) {
				// error.
				sqlite3_close(dbh);
				assert(config->aliases);
				return -1;
			}
			else {
				if (config_load_paths(config, dbh) < 0) {
					// error.
					sqlite3_close(dbh);
					return -1;
				}
			}
		}

		// close the database connection.
		sqlite3_close(dbh);
		
		return(0);
	}
}





static void settings_init(settings_t *ptr)
{
	assert(ptr != NULL);

	ptr->verbose = false;
	ptr->daemonize = false;
	ptr->username = NULL;
	ptr->pid_file = NULL;

	ptr->configfile = NULL;
	ptr->queue = NULL;

	ptr->controllers = (list_t *) malloc(sizeof(list_t));
	ll_init(ptr->controllers);
}

static void settings_free(settings_t *ptr)
{
	assert(ptr != NULL);

	if (ptr->configfile) { free(ptr->configfile); ptr->configfile = NULL; }
	if (ptr->queue) { free(ptr->queue); ptr->queue = NULL; }

	assert(ptr->controllers);
	ll_free(ptr->controllers);
	free(ptr->controllers);
	ptr->controllers = NULL;
}




//-----------------------------------------------------------------------------
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// need to initiate an RQ shutdown.
	assert(control->rq);
	rq_shutdown(control->rq);

	// delete the signal events.
	assert(control->sigint_event);
	event_free(control->sigint_event);
	control->sigint_event = NULL;

	assert(control->sighup_event);
	event_free(control->sighup_event);
	control->sighup_event = NULL;
}


//-----------------------------------------------------------------------------
// When SIGHUP is received, we need to re-load the config database.  At the
// same time, we should flush all caches and buffers to reduce the system's
// memory footprint.   It should be as close to a complete app reset as
// possible.
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
 	control_t *control = (control_t *) arg;

	assert(arg);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
	assert(0);

}





//-----------------------------------------------------------------------------
// Handle the message that was sent over the queue.  the message itself uses
// the RISP method, so we pass the data on to the risp processor.
static void message_handler(rq_message_t *msg, void *arg)
{
	int processed;
	control_t *control;

	assert(msg);
	
	control = (control_t *) arg;
	assert(control);

	// since we will only be processing one request at a time, and there are no
	// paths for blocking when processing the request, we will put the request
	// in the control structure.  If we were processing more than one, we would
	// create a list of pending requests which contain the control structure in
	// it.
	assert(control->req == NULL);
	control->req = msg;

	assert(control->rq == msg->rq);

	assert(control->risp);
	assert(msg->data);
	processed = risp_process(control->risp, control, BUF_LENGTH(msg->data), (risp_char_t *) BUF_DATA(msg->data));
	assert(processed == BUF_LENGTH(msg->data));

	// we need to get the reply and return it.  Has that been done?
	assert(0);

	// is this correct?   
	assert(0);

	control->host = NULL;
	control->path = NULL;
	control->host_len = 0;
	control->path_len = 0;
	control->req = NULL;
}


static void cmdNop(control_t *ptr) 
{
	assert(ptr != NULL);
}

static void cmdInvalid(control_t *ptr, void *data, risp_length_t len)
{
	// this callback is called if we have an invalid command.  We shouldn't be receiving any invalid commands.
	unsigned char *cast;

	assert(ptr != NULL);
	assert(data != NULL);
	assert(len > 0);
	
	cast = (unsigned char *) data;
	printf("Received invalid (%d)): [%d, %d, %d]\n", len, cast[0], cast[1], cast[2]);
	assert(0);
}

// This callback function is to be fired when the CMD_CLEAR command is 
// received.  It should clear off any data received and stored in variables 
// and flags.  In otherwords, after this is executed, the node structure 
// should be in a predictable state.
static void cmdClear(control_t *ptr) 
{
 	assert(ptr);

 	// clear the host and path info.
 	assert(0);
}

static char * check_path(list_t *paths, int path_len, char *pathstr)
{
	char *queue = NULL;
	int count;
	void *next;
	config_path_t *path;

	assert(paths);
	assert(path_len > 0);
	assert(pathstr);

	count = 0;
	assert(paths);
	next = ll_start(paths);
	path = ll_next(paths, &next);
	while (path) {
		assert(path->length > 0);
		assert(path->path);
		assert(queue == NULL);

		count++;

		if (path_len == path->length) {
			if (strncmp(pathstr, path->path, path->length) == 0) {
				queue = path->consumer;
				assert(queue);
		
				// if the path is not at the top of the list, we will move it to
				// the top so that it will be found faster in the future.
				if (count > 1) {
					ll_remove(paths, path, &next);
					ll_push_head(paths, path);
				}
			}
		}

		if (queue == NULL) { path = ll_next(paths, &next); }
		else               { path = NULL; }
	}

	return(queue);
}

static void parse_path(list_t *list, char *path)
{
	char *argument, *next;

	assert(list);
	assert(path);
	 
  argument = path;

	assert(argument[0] == '/');
	argument++;
 
  next = argument;
  while (next != NULL && *next != '\0') {
  	argument = strsep(&next, "/");
  	ll_push_tail(list, argument);
	}

	assert(ll_count(list) > 0);
}

static void add_path(list_t *list, char *str, int len, char *queue)
{
	config_path_t *path;

	assert(list);
	assert(str);
	assert(len > 0);
	assert(queue);
	
	// add the path to the list so that we dont have to go through all this again.
	path = (config_path_t *) malloc(sizeof(config_path_t));

	path->path = (char *) malloc(len + 2);
	memmove(path->path, str, len);
	path->path[len] = '\0';
	path->length = len;
								
	path->consumer = strdup(queue);
	ll_push_head(list, path);
	path = NULL;
}



// This callback function is called when the CMD_EXECUTE command is received.  
// It should look at the data received so far, and figure out what operation 
// needs to be done on that data.
static void cmdExecute(control_t *ptr) 
{
	config_alias_t *alias;
	config_host_t *host;
	char *queue;
	void *next;
	int count;
	int redirect;
	char *tmppath = NULL;
	list_t *list_path;
	list_t *list_leftover;
	char *segment;
	char *joined;

 	assert(ptr);
 	assert(ptr->host);
 	assert(ptr->path);
 	assert(ptr->req);

	// check that we have both host and path.
	if (ptr->host_len > 0 && ptr->path_len > 0) {
		
		// find the host object.
		assert(ptr->host);
		assert(ptr->config);
		assert(ptr->config->aliases);

		count = 0;
		host = NULL;
		next = ll_start(ptr->config->aliases);
		alias = ll_next(ptr->config->aliases, &next);
		while (alias) {
			assert(alias->alias);
			assert(alias->length > 0);

			count ++;

			if (ptr->host_len == alias->length) {
				if (strncasecmp(ptr->host, alias->alias, alias->length) == 0) {
					// hosts match.
					host = alias->host;
					assert(host);

					// if the alias is not at the top of the list, we will move it to
					// the top of the list to improve searching speed for the most used
					// host names.
					if (count > 1) {
						ll_remove(ptr->config->aliases, alias, &next);
						ll_push_head(ptr->config->aliases, alias);
					}

					// we found what we are looking for, we dont need to search through the list anymore.
					alias = NULL;
				}
				else {
					alias = ll_next(ptr->config->aliases, &next);
				}
			}
			else {
				alias = ll_next(ptr->config->aliases, &next);
			}
		}
	
		// check the config tables for the host/path combo.
		if (host) {

			assert(ptr->path_len > 0);
			assert(ptr->path);
			queue = NULL;
			
			// first we will search for the exact path.  If we dont find that, then
			// we will have to remove segments from the path and keep trying until
			// we find what we are looking for, or not.
			// 
			// go thru the list of paths.
			// if we find what we are looking for, queue will contain something.
			// move path to the head of the list.
			queue = check_path(host->paths, ptr->path_len, ptr->path);


			// if queue is null, then we didn't find what we are looking for with a direct search.
			redirect = 0;
			if (queue == NULL) {

				// check to see if the path is '/', because that would mean the path's
				// aren't specific so we should send all queries to a particular host.
				if (ptr->path_len == 1 && ptr->path[0] == '/') {
					assert(host->consumer);
					queue = host->consumer;

					// add this path to the paths list, so that it is found quicker next time.
					add_path(host->paths, "/", 1, queue);
				}
				else {
					
					// then temporarily add a '/' to the end of the path and see if that is in there.
					// if it is, we have a queue.
					// move path to the head of the list.
					// return a REDIRECT with the correct URL.
					assert(ptr->path_len > 0);
					tmppath = (char *) malloc(ptr->path_len + 2);
					memmove(tmppath, ptr->path, ptr->path_len);
					tmppath[ptr->path_len] = '\0';

					assert(ptr->path_len > 0);
					if (ptr->path[ptr->path_len-1] != '/') {
						// add a '/' to the path, and check again.
						strcat(tmppath, "/");
						queue = check_path(host->paths, ptr->path_len+1, tmppath);

						// remove the '/' we had added.
						tmppath[ptr->path_len] = '\0';
						
						// need to redirect.
						if (queue) {
							redirect = 1;
							assert(0);
						}
					}

					if (queue) {
						// this means that we found the directory already. but we cant use
						// this queue because it is not correct.  We need to use tmppath.
						assert(0);
					}
					else {

						list_path = (list_t *) malloc(sizeof(list_t));
						list_leftover = (list_t *) malloc(sizeof(list_t));
						ll_init(list_path);
						ll_init(list_leftover);

						// parse the path, breaking it up into segments, and add each segment to the list_path.
						parse_path(list_path, tmppath);
						
		
						// if queue is still null, remove the last segment and add it to a list.
						// search the paths again.
						// keep looping until we run out of segments, or we find a queue.
						// add the path, and the 'leftover' to the head of the paths list.
						segment = ll_pop_tail(list_path);
						while (segment && queue == NULL) {
							ll_push_head(list_leftover, segment);

							// join the strings in the list together.  We cant just take
							// that as it is, because we need to add the '/' to the end
							// of it.
							joined = strdup(ll_join_str(list_path, "/"));
							joined = (char *) realloc(joined, strlen(joined) + 2);
							strcat(joined, "/");
							assert(joined);
							
							queue = check_path(host->paths, strlen(joined), joined);

							if (queue) {
								// add the path to the list so that we dont have to go through all this again.
								add_path(host->paths, ptr->path, ptr->path_len, queue);
							}

							// make sure we free the 'joined' value that we created.
							free(joined);
							joined = NULL;
						}
				
						// if we still cant find the path.
						// return the main 'host' path.
						// add the path, and the 'leftover' to the head of the paths list.
						if (queue == NULL) {
							queue = host->consumer;
							if (queue) {
								// add the path to the list so that we dont have to go through all this again.
								add_path(host->paths, ptr->path, ptr->path_len, queue);
							}
						}

						while ((ll_pop_head(list_path)));
						ll_free(list_path);
						free(list_path);
						list_path = NULL;

						while ((ll_pop_head(list_leftover)));
						ll_free(list_leftover);
						free(list_leftover);
						list_leftover = NULL;
					}
				}
			}

			// Now we need to actually send the info we have calculated.
			if (queue && redirect == 1) {
				// we have a queue, but we cant use it.  Send redirect info back.
				assert(0);
			}
			else if (queue) {
				// we have a queue.
				// we might also have 'leftover'.
				assert(0);
			}
			else {
				// we dont have a queue.  The information is not available.
				assert(0);
			}
		}

		if (tmppath) {
			free(tmppath);
		}
	}
}


static void cmdHost(control_t *ptr, risp_length_t length, risp_char_t *data)
{
	assert(ptr);
	assert(length > 0 && length < 256);
	assert(data != NULL);

	// save the host info somewhere
	ptr->host = data;
	ptr->host_len = length;
}

static void cmdPath(control_t *ptr, risp_length_t length, risp_char_t *data)
{
	assert(ptr);
	assert(length > 0);
	assert(data != NULL);
	
	// save the path info somewhere
	ptr->path = data;
	ptr->path_len = length;
}



//-----------------------------------------------------------------------------
// Create and init our controller object.   The  control object will have
// everything in it that is needed to run this server.  It is in this object
// because we need to pass a pointer to the handler that will be doing the work.
static void init_control(control_t *control)
{
	assert(control != NULL);

	control->rq = NULL;
	control->risp = NULL;
	control->settings = NULL;
	control->bufpool = NULL;

	control->sigint_event = NULL;
	control->sighup_event = NULL;

	control->req = NULL;
	control->host = NULL;
	control->host_len = 0;
	control->path = NULL;
	control->path_len = 0;
	
	control->reply = (expbuf_t *) malloc(sizeof(expbuf_t));
	expbuf_init(control->reply, 0);
}

static void cleanup_control(control_t *control)
{
	assert(control != NULL);

	assert(control->reply);
	expbuf_clear(control->reply);
	expbuf_free(control->reply);
	free(control->reply);
	control->reply = NULL;

	control->host = NULL;
	control->path = NULL;
	control->host_len = 0;
	control->path_len = 0;

	assert(control->req == NULL);
	assert(control->bufpool == NULL);
	assert(control->settings == NULL);
	assert(control->rq == NULL);
	assert(control->risp == NULL);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
}

static void init_settings(control_t *control)
{
	assert(control);
	assert(control->settings == NULL);
	control->settings = (settings_t *) malloc(sizeof(settings_t));
	settings_init(control->settings);
}

static void cleanup_settings(control_t *control)
{
	assert(control);
	assert(control->settings);
	settings_free(control->settings);
	free(control->settings);
	control->settings = NULL;
}

//-----------------------------------------------------------------------------
// Check the settings that we have received and generate an error if we dont
// have enough.
static void check_settings(control_t *control)
{
	assert(control);
	assert(control->settings);

	assert(control->settings->controllers);
	if (ll_count(control->settings->controllers) == 0) {
		fprintf(stderr, "Need at least one controller specified.\n");
		exit(EXIT_FAILURE);
	}

	if (control->settings->queue == NULL) {
		fprintf(stderr, "Need to specify a queue.\n");
		exit(EXIT_FAILURE);
	}
	
	if (control->settings->configfile == NULL) {
		fprintf(stderr, "Need to specify a path for the config db file.\n");
		exit(EXIT_FAILURE);
	}
}

//-----------------------------------------------------------------------------
// If we need to run as a daemon, then do so, dropping privs to a specific
// username if it was specified, and creating a pid file, if that was
// specified.
static void init_daemon(control_t *control)
{
	assert(control);
	assert(control->settings);

	if (control->settings->daemonize) {
		if (rq_daemon(control->settings->username, control->settings->pid_file, control->settings->verbose) != 0) {
			fprintf(stderr, "failed to daemon() in order to daemonize\n");
			exit(EXIT_FAILURE);
		}
	}
}
	
// remove the PID file if we're a daemon
static void cleanup_daemon(control_t *control)
{
	assert(control);
	assert(control->settings);
	
	if (control->settings->daemonize && control->settings->pid_file) {
		assert(control->settings->pid_file[0] != 0);
		unlink(control->settings->pid_file);
	}
}

//-----------------------------------------------------------------------------
static void init_events(control_t *control)
{
	assert(control->evbase == NULL);
	control->evbase = event_base_new();
	assert(control->evbase);
}

//-----------------------------------------------------------------------------
static void cleanup_events(control_t *control)
{
	assert(control);
	assert(control->evbase);

	event_base_free(control->evbase);
	control->evbase = NULL;
}


//-----------------------------------------------------------------------------
static void init_rq(control_t *control)
{
	assert(control);
	assert(control->rq == NULL);

	control->rq = (rq_t *) malloc(sizeof(rq_t));
	rq_init(control->rq);

	assert(control->evbase);
	assert(control->rq);
	rq_setevbase(control->rq, control->evbase);
}

//-----------------------------------------------------------------------------
static void cleanup_rq(control_t *control)
{
	assert(control);
	assert(control->rq);

	rq_cleanup(control->rq);
	free(control->rq);
	control->rq = NULL;
}


//-----------------------------------------------------------------------------
static void init_signals(control_t *control)
{
	assert(control);
	assert(control->evbase);
	
	assert(control->sigint_event == NULL);
	control->sigint_event = evsignal_new(control->evbase, SIGINT, sigint_handler, control);
	assert(control->sigint_event);
	event_add(control->sigint_event, NULL);

	assert(control->sighup_event == NULL);
	control->sighup_event = evsignal_new(control->evbase, SIGINT, sighup_handler, control);
	assert(control->sighup_event);
	event_add(control->sighup_event, NULL);
}

//-----------------------------------------------------------------------------
static void cleanup_signals(control_t *control)
{
	assert(control);
	assert(control->sigint_event == NULL);
	assert(control->sighup_event == NULL);
}

//-----------------------------------------------------------------------------
// add the controller details to the rq library so taht it can manage
// connecting to the controller.
static void init_controllers(control_t *control)
{
	char *str;
	
	assert(control);
	assert(control->settings);
	assert(control->settings->controllers);
	assert(control->rq);

	while ((str = ll_pop_head(control->settings->controllers))) {
		rq_addcontroller(control->rq, str);
		free(str);
	}
}

//-----------------------------------------------------------------------------
static void cleanup_controllers(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->controllers);
	assert(ll_count(control->settings->controllers) == 0);
}

//-----------------------------------------------------------------------------
static void init_config(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->configfile);
	assert(control->config == NULL);

	control->config = (config_t *) malloc(sizeof(config_t));
	assert(control->config);
	config_init(control->config);
	if (config_load(control->config, control->settings->configfile) < 0) {
		fprintf(stderr, "Errors loading config file: %s\n", control->settings->configfile);
		exit(EXIT_FAILURE);
	}
}

//-----------------------------------------------------------------------------
static void cleanup_config(control_t *control)
{
	assert(control);
	assert(control->config);
	config_free(control->config);
	free(control->config);
	control->config = NULL;
}

//-----------------------------------------------------------------------------
static void init_bufpool(control_t *control)
{
	assert(control);
	assert(control->bufpool == NULL);

	control->bufpool = (expbuf_pool_t *) malloc(sizeof(expbuf_pool_t));
	expbuf_pool_init(control->bufpool, 0);
}

//-----------------------------------------------------------------------------
static void cleanup_bufpool(control_t *control)
{
	assert(control);
	assert(control->bufpool);

	expbuf_pool_free(control->bufpool);
	free(control->bufpool);
	control->bufpool = NULL;
}

//-----------------------------------------------------------------------------
// connect to the specific queues that we want to connect to.
static void init_queues(control_t *control)
{
	assert(control);
	assert(control->settings);
	assert(control->settings->queue);
	assert(control->rq);
	
	rq_consume(control->rq, control->settings->queue, 200, RQ_PRIORITY_NORMAL, 0, message_handler, control);
}

static void cleanup_queues(control_t *control)
{
	assert(control);

	// not really anything we can do about the queues, they get cleaned up automatically by RQ.
}

// Initialise the risp system.
static void init_risp(control_t *control)
{
	assert(control);
	assert(control->risp == NULL);

	control->risp = risp_init();
	assert(control->risp != NULL);
	risp_add_invalid(control->risp, cmdInvalid);
	risp_add_command(control->risp, HCFG_CMD_CLEAR, 	 &cmdClear);
	risp_add_command(control->risp, HCFG_CMD_EXECUTE,  &cmdExecute);
 	risp_add_command(control->risp, HCFG_CMD_HOST,     &cmdHost);
	risp_add_command(control->risp, HCFG_CMD_PATH,     &cmdPath);
}

// cleanup risp library.
static void cleanup_risp(control_t *control)
{
	assert(control);
	assert(control->risp);

	risp_shutdown(control->risp);
	control->risp = NULL;
}





void process_args(settings_t *settings, int argc, char **argv)
{
	int c;
	
	while (-1 != (c = getopt(argc, argv,
			"h"   /* help */
			"v"   /* verbose */
			"d:"  /* start as daemmon */
			"u:"  /* username to run as */
			"P:"  /* pidfile to store pid to */
			"c:"	/* controller to connect to */
			"f:"  /* sqlite3 configuration filename */
			"q:"  /* queue to listen on */
		))) {
		switch (c) {

			case 'q':
				assert(settings->queue == NULL);
				settings->queue = strdup(optarg);
				assert(settings->queue);
				break;

			case 'f':
				assert(settings->configfile == NULL);
				settings->configfile = strdup(optarg);
				assert(settings->configfile);
				break;

			case 'c':
				assert(settings->controllers);
				ll_push_tail(settings->controllers, strdup(optarg));
				break;

			case 'h':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'v':
				settings->verbose++;
				break;
			case 'd':
				assert(settings->daemonize == false);
				settings->daemonize = true;
				break;
			case 'u':
				assert(settings->username == NULL);
				settings->username = strdup(optarg);
				assert(settings->username != NULL);
				break;
			case 'P':
				assert(settings->pid_file == NULL);
				settings->pid_file = strdup(optarg);
				assert(settings->pid_file != NULL);
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				exit(EXIT_FAILURE);
				break;
		}
	}
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	control_t      *control  = NULL;

///============================================================================
/// Initialization.
///============================================================================

	control = (control_t *) malloc(sizeof(control_t));

	init_control(control);
	init_settings(control);

	process_args(control->settings, argc, argv);

	check_settings(control);
	init_daemon(control);
	init_events(control);
	
	init_rq(control);
	init_risp(control);
	init_signals(control);
	init_controllers(control);
	init_config(control);
	init_bufpool(control);
	init_queues(control);

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  Once inside the
	// rq_process function, everything is initiated by the RQ event system.
	assert(control != NULL);
	assert(control->evbase);
	event_base_loop(control->evbase, 0);

///============================================================================
/// Shutdown
///============================================================================

	cleanup_events(control);

	cleanup_queues(control);
	cleanup_bufpool(control);
	cleanup_config(control);
	cleanup_controllers(control);
	cleanup_signals(control);
	cleanup_risp(control);
	cleanup_rq(control);

	cleanup_daemon(control);
	cleanup_settings(control);
	cleanup_control(control);

	free(control);

	return 0;
}


