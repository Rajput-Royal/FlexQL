/*
 * net.c - Reliable read/write helpers + wire protocol framing.
 */
#ifndef FLEXQL_NET_C
#define FLEXQL_NET_C

#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

static int read_exact(int fd, void *buf, size_t n) {
    size_t got=0;
    while (got<n) { ssize_t r=read(fd,(char*)buf+got,n-got); if (r<=0) return -1; got+=(size_t)r; }
    return 0;
}

static int write_exact(int fd, const void *buf, size_t n) {
    size_t sent=0;
    while (sent<n) { ssize_t w=write(fd,(const char*)buf+sent,n-sent); if (w<=0) return -1; sent+=(size_t)w; }
    return 0;
}

static int send_u32(int fd, uint32_t v) { uint32_t n=htonl(v); return write_exact(fd,&n,4); }
static int recv_u32(int fd, uint32_t *v) { uint32_t n; if (read_exact(fd,&n,4)!=0) return -1; *v=ntohl(n); return 0; }

/* Used by server sending row data */
static int send_str(int fd, const char *s) __attribute__((unused));
static int send_str(int fd, const char *s) {
    uint32_t len=s?(uint32_t)strlen(s):0;
    if (send_u32(fd,len)!=0) return -1;
    if (len) return write_exact(fd,s,len);
    return 0;
}

/* Used by client receiving row data */
static char *recv_str(int fd) __attribute__((unused));
static char *recv_str(int fd) {
    uint32_t len; if (recv_u32(fd,&len)!=0) return NULL;
    char *buf=calloc(len+1,1);
    if (len && read_exact(fd,buf,len)!=0) { free(buf); return NULL; }
    return buf;
}

#endif /* FLEXQL_NET_C */
