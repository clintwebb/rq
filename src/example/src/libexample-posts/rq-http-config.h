#ifndef __RQ_HTTP_CONFIG_H
#define __RQ_HTTP_CONFIG_H


#define RQ_HTTP_CONFIG_VERSION  0x00011500
#define RQ_HTTP_CONFIG_VERSION_TEXT  "v1.15.00"


#include <linklist.h>
#include <risp.h>
#include <rq.h>


#if (LIBLINKLIST_VERSION < 0x00008000)
	#error "Requires liblinklist v0.80 or higher"
#endif


#if (RISP_VERSION < 0x00010010)
	#error "Requires librisp v1.00.10 or higher."
#endif



//                                          commands - null param (0 to 31)
#define HCFG_CMD_NOP              0
#define HCFG_CMD_CLEAR            1
#define HCFG_CMD_LOOKUP           2
#define HCFG_CMD_RESULT           3
#define HCFG_CMD_FAILED           4
//                                          flags - null param (32 to 63)
//                                          byte integer (64 to 95)
//                                          short integer (96 to 127)
//                                          large integer (128 to 159)
//                                          short string (160 to 192)
#define HCFG_CMD_QUEUE            160
//                                          string (192 to 223)
#define HCFG_CMD_HOST             192
#define HCFG_CMD_PATH             193
#define HCFG_CMD_LEFTOVER         194
#define HCFG_CMD_REDIRECT         195
//                                          large string (224 to 255)


typedef int rq_hcfg_id_t;



typedef struct {
  rq_t *rq;
  risp_t *risp;
  char *queue;
  int expiry;
  list_t *cache;		/// entry_t
  list_t *waiting;	/// waiting_t
} rq_hcfg_t;



void rq_hcfg_init(rq_hcfg_t *cfg, rq_t *rq, const char *queue, int expiry);
void rq_hcfg_free(rq_hcfg_t *cfg);

rq_hcfg_id_t rq_hcfg_lookup(
	rq_hcfg_t *cfg,
	const char *host,
	const char *path,
	void (*handler)(const char *queue, const char *path, const char *leftover, const char *redirect, void *arg),
	void *arg);

void rq_hcfg_cancel(rq_hcfg_t *cfg, rq_hcfg_id_t id);

#endif
