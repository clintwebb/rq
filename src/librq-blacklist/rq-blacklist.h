#ifndef __RQ_BLACKLIST_H
#define __RQ_BLACKLIST_H

#define RQ_BLACKLIST_VERSION  0x00010500
#define RQ_BLACKLIST_VERSION_TEXT  "v1.05.00"

#include <linklist.h>
#include <risp.h>
#include <rq.h>

#if (LIBLINKLIST_VERSION < 0x00007000)
	#error "liblinklist needs to be version v0.70 or higher"
#endif


//-----------------------------------------------------------------------------
// RISP Commands
//-----------------------------------------------------------------------------
//                                          execute command (0 to 31)
#define BL_CMD_NOP              0
#define BL_CMD_CLEAR            1
#define BL_CMD_CHECK            2
#define BL_CMD_DENY             3
#define BL_CMD_ACCEPT           4
//                                          flag byte (32 to 63)
//                                          byte integer (64 to 95)
//                                          short integer (96 to 127)
//                                          large integer (128 to 159)
#define BL_CMD_IP               128
//                                          short string (160 to 192)
//                                          string (192 to 223)
//                                          large string (224 to 255)
//-----------------------------------------------------------------------------


typedef int rq_blacklist_id_t;
typedef int rq_blacklist_status_t;

#define BLACKLIST_ACCEPT	0
#define BLACKLIST_DENY		1

typedef struct {
  rq_t *rq;
  char *queue;
  int expires;
  list_t *cache;		/// cache_entry_t
  list_t *waiting;	/// cache_waiting_t
} rq_blacklist_t;


void rq_blacklist_init(rq_blacklist_t *blacklist, rq_t *rq, const char *queue, int expires);
void rq_blacklist_free(rq_blacklist_t *blacklist);

rq_blacklist_id_t rq_blacklist_check(rq_blacklist_t *blacklist, struct sockaddr *address, int socklen, void (*handler)(rq_blacklist_status_t status, void *arg), void *arg);
void rq_blacklist_cancel(rq_blacklist_t *blacklist, rq_blacklist_id_t id);


#endif
