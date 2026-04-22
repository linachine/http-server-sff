#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>

#define port 8080
#define chunksize 4096
#define bufsize 1024
#define reqsize (bufsize * 4)

/* Global sequence counter and logging mutex */
static int global_seq = 0;
static pthread_mutex_t seq_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mtx = PTHREAD_MUTEX_INITIALIZER;

/* Request structure */
typedef struct {
    int clientfd;
    int seq;
    char path[256];
    long filesize;
    struct timeval arrival;
} request;

/* Format timestamp in ISO 8601 format */
static void format_time(struct timeval *tv, char *buf, size_t len) {
    time_t sec = tv->tv_sec;
    struct tm *tm_info = localtime(&sec);
    
    char temp[64];
    strftime(temp, sizeof(temp), "%Y-%m-%dT%H:%M:%S", tm_info);
    snprintf(buf, len, "%s.%03ld", temp, tv->tv_usec / 1000);
}

/* Log request arrival - with sampling to avoid overwhelming output */
static void log_request(int seq, const char *path, struct timeval *arrival) {
    /* Always log first 100, then every 10th request */
    if (seq <= 100 || seq % 10 == 0) {
        char timebuf[64];
        format_time(arrival, timebuf, sizeof(timebuf));
        
        pthread_mutex_lock(&log_mtx);
        printf("REQUEST seq=%d path=\"%s\" time=%s\n", seq, path, timebuf);
        fflush(stdout);
        pthread_mutex_unlock(&log_mtx);
    }
}

/* Log worker pickup - with sampling */
static void log_worker(int worker_id, int seq, long filesize) {
    /* Always log first 100, then every 10th request */
    if (seq <= 100 || seq % 10 == 0) {
        pthread_mutex_lock(&log_mtx);
        printf("WORKER %d picked request with seq=%d size=%ld\n", worker_id, seq, filesize);
        fflush(stdout);
        pthread_mutex_unlock(&log_mtx);
    }
}

/* Get next sequence number (thread-safe) */
static int next_seq() {
    pthread_mutex_lock(&seq_mtx);
    int seq = ++global_seq;
    pthread_mutex_unlock(&seq_mtx);
    return seq;
}

static int sendall(int fd, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = (const char *)buf;

    while (total < len) {
        ssize_t sent = send(fd, p + total, len - total, 0);
        if (sent <= 0) return -1;
        total += (size_t)sent;
    }
    return 0;
}

static const char *contenttype(const char *name) {
    if (strstr(name, ".html")) return "text/html";
    if (strstr(name, ".txt")) return "text/plain";
    return "application/octet-stream";
}

/*  Better path parsing */
static int parsereq(const char *req, char *path, size_t pathlen) {
    const char *lineend = strstr(req, "\r\n");
    size_t linelen = lineend ? (size_t)(lineend - req) : strlen(req);

    const char *sp1 = memchr(req, ' ', linelen);
    if (!sp1) return 400;

    size_t mlen = (size_t)(sp1 - req);
    if (mlen != 3 || strncmp(req, "GET", 3) != 0) return 405;

    const char *pstart = sp1 + 1;
    const char *sp2 = memchr(pstart, ' ', linelen - (size_t)(pstart - req));
    if (!sp2) return 400;

    size_t plen = (size_t)(sp2 - pstart);
    if (plen == 0) return 400;

    if (pstart[0] != '/') return 400;

    /* Default page */
    if (plen == 1) {
        snprintf(path, pathlen, "index.html");
        return 200;
    }

    /* Copy path, skipping the leading '/' */
    plen -= 1;
    if (plen >= pathlen) return 414;

    memcpy(path, pstart + 1, plen);
    path[plen] = '\0';
    
    return 200;
}

static int badpath(const char *path) {
    if (path[0] == '\0') return 1;
    if (strstr(path, "..")) return 1;
    if (strchr(path, '\\')) return 1;
    /* Also reject paths that still have leading slash */
    if (path[0] == '/') return 1;
    return 0;
}

static void sendtext(int fd, int code, const char *msg) {
    const char *status = "500 Internal Server Error";
    if (code == 200) status = "200 OK";
    else if (code == 400) status = "400 Bad Request";
    else if (code == 404) status = "404 Not Found";
    else if (code == 405) status = "405 Method Not Allowed";
    else if (code == 414) status = "414 URI Too Long";

    char resp[512];
    size_t mlen = strlen(msg);

    int n = snprintf(resp, sizeof(resp),
        "HTTP/1.0 %s\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %zu\r\n"
        "\r\n"
        "%s",
        status, mlen, msg
    );

    if (n > 0) sendall(fd, resp, (size_t)n);
}

/* Serve file to client */
static void handleclient(int clientfd, const char *path) {
    FILE *file = fopen(path, "rb");
    if (!file) {
        sendtext(clientfd, 404, "file not found");
        return;
    }

    fseek(file, 0, SEEK_END);
    long filesize = ftell(file);
    fseek(file, 0, SEEK_SET);

    if (filesize < 0) {
        fclose(file);
        sendtext(clientfd, 500, "server error");
        return;
    }

    const char *type = contenttype(path);
    char hdr[512];
    int hn = snprintf(hdr, sizeof(hdr),
        "HTTP/1.0 200 OK\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %ld\r\n"
        "\r\n",
        type, filesize
    );

    if (hn > 0) {
        if (sendall(clientfd, hdr, (size_t)hn) < 0) {
            fclose(file);
            return;
        }
    }

    char chunk[chunksize];
    while (1) {
        size_t n = fread(chunk, 1, sizeof(chunk), file);
        if (n == 0) break;
        if (sendall(clientfd, chunk, n) < 0) break;
    }

    fclose(file);
}

/* Queue with SFF scheduling */
typedef struct {
    request *reqs;
    int cap;
    int count;
    pthread_mutex_t mtx;
    pthread_cond_t notempty;
    pthread_cond_t notfull;
} queue;

static void queueinit(queue *q, int cap) {
    q->reqs = (request *)malloc((size_t)cap * sizeof(request));
    q->cap = cap;
    q->count = 0;
    pthread_mutex_init(&q->mtx, 0);
    pthread_cond_init(&q->notempty, 0);
    pthread_cond_init(&q->notfull, 0);
}

static void queuepush(queue *q, request *req) {
    pthread_mutex_lock(&q->mtx);
    while (q->count == q->cap) pthread_cond_wait(&q->notfull, &q->mtx);
    
    q->reqs[q->count] = *req;
    q->count++;
    
    pthread_cond_signal(&q->notempty);
    pthread_mutex_unlock(&q->mtx);
}

/* SFF Scheduling - find and remove smallest file */
static request queuepop(queue *q) {
    pthread_mutex_lock(&q->mtx);
    while (q->count == 0) pthread_cond_wait(&q->notempty, &q->mtx);
    
    /* Find request with smallest file size */
    int min_idx = 0;
    long min_size = q->reqs[0].filesize;
    
    for (int i = 1; i < q->count; i++) {
        if (q->reqs[i].filesize < min_size) {
            min_size = q->reqs[i].filesize;
            min_idx = i;
        }
    }
    
    /* Extract the smallest request */
    request result = q->reqs[min_idx];
    
    /* Shift remaining items to fill the gap */
    for (int i = min_idx; i < q->count - 1; i++) {
        q->reqs[i] = q->reqs[i + 1];
    }
    q->count--;
    
    pthread_cond_signal(&q->notfull);
    pthread_mutex_unlock(&q->mtx);
    
    return result;
}

typedef struct {
    queue *q;
    int id;
} workerarg;

/* Worker thread with logging */
static void *worker(void *arg) {
    workerarg *wa = (workerarg *)arg;

    while (1) {
        request req = queuepop(wa->q);
        
        log_worker(wa->id, req.seq, req.filesize);
        
        handleclient(req.clientfd, req.path);
        close(req.clientfd);
    }

    return 0;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    int nthreads = 4;
    int qsize = 16;
    if (argc >= 2) nthreads = atoi(argv[1]);
    if (argc >= 3) qsize = atoi(argv[2]);
    if (nthreads <= 0) nthreads = 4;
    if (qsize <= 0) qsize = 16;

    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverfd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    serveraddr.sin_port = htons(port);

    if (bind(serverfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
        perror("bind");
        close(serverfd);
        return 1;
    }

    if (listen(serverfd, 128) < 0) {
        perror("listen");
        close(serverfd);
        return 1;
    }

    queue q;
    queueinit(&q, qsize);

    pthread_t *threads = (pthread_t *)malloc((size_t)nthreads * sizeof(pthread_t));
    workerarg *args = (workerarg *)malloc((size_t)nthreads * sizeof(workerarg));

    for (int i = 0; i < nthreads; i++) {
        args[i].q = &q;
        args[i].id = i + 1;

        if (pthread_create(&threads[i], 0, worker, &args[i]) != 0) {
            perror("pthread_create");
            close(serverfd);
            return 1;
        }
    }

    printf("server http://localhost:%d threads=%d queue=%d\n", port, nthreads, qsize);

    while (1) {
        struct sockaddr_in clientaddr;
        socklen_t clientlen = sizeof(clientaddr);

        int clientfd = accept(serverfd, (struct sockaddr *)&clientaddr, &clientlen);
        if (clientfd < 0) {
            perror("accept");
            continue;
        }

        struct timeval arrival;
        gettimeofday(&arrival, NULL);

        char buf[bufsize];
        char reqbuf[reqsize];
        int reqlen = 0;

        memset(reqbuf, 0, sizeof(reqbuf));
        while (reqlen < (int)sizeof(reqbuf) - 1) {
            int bytes = (int)recv(clientfd, buf, sizeof(buf) - 1, 0);
            if (bytes <= 0) break;

            buf[bytes] = '\0';

            size_t left = sizeof(reqbuf) - (size_t)reqlen - 1;
            size_t add = (size_t)bytes;
            if (add > left) add = left;

            memcpy(reqbuf + reqlen, buf, add);
            reqlen += (int)add;
            reqbuf[reqlen] = '\0';

            if (strstr(reqbuf, "\r\n\r\n")) break;
        }

        char path[256];
        int code = parsereq(reqbuf, path, sizeof(path));
        
        if (code != 200 || badpath(path)) {
            if (code == 405) sendtext(clientfd, 405, "method not allowed");
            else if (code == 414) sendtext(clientfd, 414, "uri too long");
            else sendtext(clientfd, 400, "bad request");
            close(clientfd);
            continue;
        }

        /* strip leading slash if somehow present */
        if (path[0] == '/') {
            memmove(path, path + 1, strlen(path));
        }

        /* Get file size using stat */
        struct stat st;
        long filesize = -1;
        if (stat(path, &st) == 0) {
            filesize = (long)st.st_size;
        }

        /* Create request structure */
        request req;
        req.clientfd = clientfd;
        req.seq = next_seq();
        strncpy(req.path, path, sizeof(req.path) - 1);
        req.path[sizeof(req.path) - 1] = '\0';
        req.filesize = filesize;
        req.arrival = arrival;

        log_request(req.seq, req.path, &req.arrival);

        queuepush(&q, &req);
    }

    close(serverfd);
    return 0;
}