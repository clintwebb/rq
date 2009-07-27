#ifndef __RQ_HTTP_CONFIG_H
#define __RQ_HTTP_CONFIG_H


#define RQ_HTTP_CONFIG_VERSION  0x00011000
#define RQ_HTTP_CONFIG_VERSION_TEXT  "v1.10.00"


#include <rq.h>



//                                          null param (0 to 63)
#define HCFG_CMD_NOP              0
#define HCFG_CMD_CLEAR            1
#define HCFG_CMD_LOOKUP           2
#define HCFG_CMD_REDIRECT         3
//                                          byte integer (64 to 95)
//                                          short integer (96 to 127)
//                                          large integer (128 to 159)
//                                          short string (160 to 192)
#define HCFG_CMD_HOST             160
#define HCFG_CMD_QUEUE            161
//                                          string (192 to 223)
#define HCFG_CMD_PATH             192
#define HCFG_CMD_LEFTOVER         193
//                                          large string (224 to 255)


typedef int rq_hcfg_id_t;



typedef struct {
  rq_t *rq;
  char *queue;
} rq_hcfg_t;



void rq_hcfg_init(rq_hcfg_t *cfg, rq_t *rq, const char *queue);
void rq_hcfg_free(rq_hcfg_t *cfg);

rq_hcfg_id_t rq_hcfg_lookup(
	rq_hcfg_t *cfg,
	const char *host,
	const char *path,
	void (*handler)(const char *queue, void *arg),
	void *arg);

void rq_hcfg_cancel_lookup(rq_hcfg_t *cfg, rq_hcfg_id_t id);

#endif
