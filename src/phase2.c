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

static int globalseq = 0;
static pthread_mutex_t seqmtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t logmtx = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int clientfd;
    int seq;
    char path[256];
    long filesize;
    struct timeval arrival;
} request;

static void formattime(struct timeval *tv, char *buf, size_t len) {
    time_t sec = tv->tv_sec;
    struct tm *tminfo = localtime(&sec);
    char temp[64];
    strftime(temp, sizeof(temp), "%Y-%m-%dT%H:%M:%S", tminfo);
    snprintf(buf, len, "%s.%03ld", temp, (long)(tv->tv_usec / 1000));
}

static void logrequest(int seq, const char *path, struct timeval *arrival) {
    if (seq <= 100 || seq % 10 == 0) {
        char timebuf[64];
        formattime(arrival, timebuf, sizeof(timebuf));
        pthread_mutex_lock(&logmtx);
        printf("REQUEST seq=%d path=\"%s\" time=%s\n", seq, path, timebuf);
        fflush(stdout);
        pthread_mutex_unlock(&logmtx);
    }
}

static void logworker(int id, int seq, long filesize) {
    if (seq <= 100 || seq % 10 == 0) {
        pthread_mutex_lock(&logmtx);
        printf("WORKER %d picked request with seq=%d size=%ld\n", id, seq, filesize);
        fflush(stdout);
        pthread_mutex_unlock(&logmtx);
    }
}

static int nextseq() {
    pthread_mutex_lock(&seqmtx);
    int s = ++globalseq;
    pthread_mutex_unlock(&seqmtx);
    return s;
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

static int parsereq(const char *req, char *path, size_t pathlen) {
    const char *lineend = strstr(req, "\r\n");
    size_t linelen = lineend ? (size_t)(lineend - req) : strlen(req);

    const char *sp1 = (const char *)memchr(req, ' ', linelen);
    if (!sp1) return 400;

    size_t mlen = (size_t)(sp1 - req);
    if (mlen != 3 || strncmp(req, "GET", 3) != 0) return 405;

    const char *pstart = sp1 + 1;
    const char *sp2 = (const char *)memchr(pstart, ' ', linelen - (size_t)(pstart - req));
    if (!sp2) return 400;

    size_t plen = (size_t)(sp2 - pstart);
    if (plen == 0) return 400;
    if (pstart[0] != '/') return 400;

    if (plen == 1) {
        snprintf(path, pathlen, "index.html");
        return 200;
    }

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

typedef struct {
    request *reqs;
    int cap;
    int count;
    int head;
    int tail;
    pthread_mutex_t mtx;
    pthread_cond_t notempty;
    pthread_cond_t notfull;
} queue;

static void queueinit(queue *q, int cap) {
    q->reqs = (request *)malloc((size_t)cap * sizeof(request));
    q->cap = cap;
    q->count = 0;
    q->head = 0;
    q->tail = 0;
    pthread_mutex_init(&q->mtx, 0);
    pthread_cond_init(&q->notempty, 0);
    pthread_cond_init(&q->notfull, 0);
}

static void queuepush(queue *q, request *r) {
    pthread_mutex_lock(&q->mtx);
    while (q->count == q->cap) pthread_cond_wait(&q->notfull, &q->mtx);

    q->reqs[q->tail] = *r;
    q->tail = (q->tail + 1) % q->cap;
    q->count++;

    pthread_cond_signal(&q->notempty);
    pthread_mutex_unlock(&q->mtx);
}

static request queuepop(queue *q) {
    pthread_mutex_lock(&q->mtx);
    while (q->count == 0) pthread_cond_wait(&q->notempty, &q->mtx);

    request r = q->reqs[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;

    pthread_cond_signal(&q->notfull);
    pthread_mutex_unlock(&q->mtx);
    return r;
}

typedef struct {
    queue *q;
    int id;
} workerarg;

static void *worker(void *arg) {
    workerarg *wa = (workerarg *)arg;
    while (1) {
        request r = queuepop(wa->q);
        logworker(wa->id, r.seq, r.filesize);
        handleclient(r.clientfd, r.path);
        close(r.clientfd);
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
    fflush(stdout);

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

        if (path[0] == '/') {
            memmove(path, path + 1, strlen(path));
        }

        struct stat st;
        long filesize = -1;
        if (stat(path, &st) == 0) {
            filesize = (long)st.st_size;
        }

        request r;
        r.clientfd = clientfd;
        r.seq = nextseq();
        strncpy(r.path, path, sizeof(r.path) - 1);
        r.path[sizeof(r.path) - 1] = '\0';
        r.filesize = filesize;
        r.arrival = arrival;

        logrequest(r.seq, r.path, &r.arrival);
        queuepush(&q, &r);
    }

    close(serverfd);
    return 0;
}
