#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <errno.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <byteswap.h>

#include <jni.h>

#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>

#include "ibcommunication.h"

#define USE_RS	1

#define rs_socket(f,t,p)  USE_RS ? rsocket(f,t,p)  : socket(f,t,p)
#define rs_bind(s,a,l)    USE_RS ? rbind(s,a,l)    : bind(s,a,l)
#define rs_listen(s,b)    USE_RS ? rlisten(s,b)    : listen(s,b)
#define rs_connect(s,a,l) USE_RS ? rconnect(s,a,l) : connect(s,a,l)
#define rs_accept(s,a,l)  USE_RS ? raccept(s,a,l)  : accept(s,a,l)
#define rs_shutdown(s,h)  USE_RS ? rshutdown(s,h)  : shutdown(s,h)
#define rs_close(s)       USE_RS ? rclose(s)       : close(s)
#define rs_recv(s,b,l,f)  USE_RS ? rrecv(s,b,l,f)  : recv(s,b,l,f)
#define rs_send(s,b,l,f)  USE_RS ? rsend(s,b,l,f)  : send(s,b,l,f)
#define rs_recvfrom(s,b,l,f,a,al) \
	USE_RS ? rrecvfrom(s,b,l,f,a,al) : recvfrom(s,b,l,f,a,al)
#define rs_sendto(s,b,l,f,a,al) \
	USE_RS ? rsendto(s,b,l,f,a,al)   : sendto(s,b,l,f,a,al)
#define rs_poll(f,n,t)	  USE_RS ? rpoll(f,n,t)	   : poll(f,n,t)
#define rs_fcntl(s,c,p)   USE_RS ? rfcntl(s,c,p)   : fcntl(s,c,p)
#define rs_setsockopt(s,l,n,v,ol) \
	USE_RS ? rsetsockopt(s,l,n,v,ol) : setsockopt(s,l,n,v,ol)
#define rs_getsockopt(s,l,n,v,ol) \
	USE_RS ? rgetsockopt(s,l,n,v,ol) : getsockopt(s,l,n,v,ol)
#define rs_getpeername(s,a,l) \
	USE_RS ? rgetpeername(s,a,l) : getpeername(s,a,l)
#define rs_getsockname(s,a,l) \
	USE_RS ? rgetsockname(s,a,l) : getsockname(s,a,l)

#define TIMEOUT 0
#define DEBUG 0
#define TIMING 0


int do_poll(struct pollfd *fds, int timeout)
{
	int ret;

	do {
		ret = rs_poll(fds, 1, timeout);
	} while (!ret);

	return ret == 1 ? (fds->revents & (POLLERR | POLLHUP)) : ret;
}

static int maxsize = 0;

static int poll_timeout = 0;
static int keepalive = 0;
static struct addrinfo ai_hints;


int getPeerIP(JNIEnv *env, char *buf, int buf_len, int sockfd);


static void printTm(int secBefore) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    tv.tv_sec -= secBefore;
    struct tm tm = *localtime(&tv.tv_sec);

    printf("%02d:%02d:%02d-%02d", tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec / 10000);
}


static void printIP(JNIEnv *env, char * prefix, int sockfd) {
    char ipstr[INET6_ADDRSTRLEN + 33];
    ipstr[0] = '\0';
    getPeerIP(env, ipstr, INET6_ADDRSTRLEN, sockfd);
    printf("%s%s\n", prefix, ipstr);
}


jint throwException(JNIEnv *env, const char *message, const char *err) {
    jclass exClass;
    char buf[1024];
    sprintf(buf, "%s: %s", message, err);
    char *className = "ibis/ipl/IbisIOException";
    exClass = (*env)->FindClass( env, className);
    return (*env)->ThrowNew(env, exClass, buf);
}


static int send_xfer(JNIEnv *env, int sockfd, void *buf, int size)
{
    int offset, ret;
    int countm1Sent = 0;
    struct timespec tt;
    int flags = size > 65536 ? MSG_DONTWAIT : 0;
    // int flags = 0;

    for (offset = 0; offset < size; ) {
	int sz = size - offset;
	if (maxsize > 0 && sz > maxsize) sz = maxsize;
	ret = rs_send(sockfd, buf + offset, sz, flags);
	if (ret > 0) {
#if DEBUG
	    printf("rs_send(%d, 0x%x, %d) = %d (%d to go), countm1Sent = %d\n", sockfd, buf + offset, sz, ret, size - offset - ret, countm1Sent);
	    fflush(stdout);
#endif
	    countm1Sent = 0;
	    offset += ret;
	} else if (ret == 0) {
	    return -1;
	} else if (errno == EWOULDBLOCK || errno == EAGAIN) {
#if DEBUG && TIMEOUT > 0
		printf("timeout at ");
		printTm(TIMEOUT);
		printf(" rs_send(), still need to send %d bytes to sockfd %d ", size - offset, sockfd);
		printIP(env, "", sockfd);
		printf("\n");
		fflush(stdout);
#endif
	    countm1Sent++;
	    if (countm1Sent > 100) {
		tt.tv_sec = 0;
		tt.tv_nsec = 10 * countm1Sent;
		nanosleep(&tt, NULL);
	    }
	} else {
	    throwException(env, "rsend", strerror(errno));
	    return ret;
	}
    }

    return 0;
}

static int recv_xfer(JNIEnv *env, int sockfd, jobject bb, int size, int off, int fully)
{
    int offset, ret;

    if (size == 0) {
	return 0;
    }

    int limit = fully ? size : 1;
    int countm1Received = 0;
    struct timespec tt;
    int flags = limit > 65536 ? MSG_DONTWAIT : 0;

    void *fa = (*env)->GetDirectBufferAddress(env, bb);
    void *buf = fa + off;

    for (offset = 0; offset < limit; ) {
	int sz = size - offset;
	if (maxsize > 0 && sz > maxsize) sz = maxsize;
	// fprintf(stdout, "recv(%d, %p, %d, %d)\n", sockfd, buf + offset, sz, flags);
	// fflush(stdout);
	ret = rs_recv(sockfd, buf + offset, sz, flags);
	// fprintf(stdout, "recv done, result = %d\n", ret);
	// fflush(stdout);
	if (ret > 0) {
#if DEBUG
	    printf("rs_recv(%d, 0x%x, %d) = %d (%d to go), countm1Received = %d\n", sockfd, buf + offset, sz, ret, size - ret - offset, countm1Received);
	    fflush(stdout);
#endif
	    countm1Received = 0;
	    offset += ret;
	} else if (ret == 0) {
	    if (fully) {
		throwException(env, "rrecv", "end of input");
	    }
	    return 0;
	} else if (errno == EWOULDBLOCK || errno == EAGAIN) {
#if DEBUG && TIMEOUT > 0
		printf("timeout at ");
		printTm(TIMEOUT);
		printf(" rs_recv(), still expecting %d bytes from sockfd %d ", size - offset, sockfd);
		printIP(env, "", sockfd);
		printf("\n");
		fflush(stdout);
#endif
	    countm1Received++;
	    // if (countm1Received > 100) {
		// tt.tv_sec = 0;
		// tt.tv_nsec = 100 * countm1Received;
		// nanosleep(&tt, NULL);	// May invalidate buffer address (at termination)
		// fa = (*env)->GetDirectBufferAddress(env, bb);
		// if (fa == NULL) {
		//     return offset == 0 ? ret : offset;
		// }
		// buf = fa + off;
	    // }
	} else {
	    throwException(env, "rrecv", strerror(errno));
	    return ret;
	}
    }

    return offset;
}


static void set_keepalive(JNIEnv *env, int rs)
{
    int optval;
    socklen_t optlen = sizeof(optlen);

    optval = 1;
    if (rs_setsockopt(rs, SOL_SOCKET, SO_KEEPALIVE, (void *) &optval, optlen)) {
	throwException(env, "rsetsockopt SO_KEEPALIVE", strerror(errno));
	return;
    }

    optval = keepalive;
    if (rs_setsockopt(rs, IPPROTO_TCP, TCP_KEEPIDLE, (void *) &optval, optlen))
	throwException(env, "rsetsockopt SO_KEEPIDLE", strerror(errno));
/*
    if (!(rs_getsockopt(rs, SOL_SOCKET, SO_KEEPALIVE, &optval, &optlen)))
	printf("Keepalive: %s\n", (optval ? "ON" : "OFF"));

    if (!(rs_getsockopt(rs, IPPROTO_TCP, TCP_KEEPIDLE, &optval, &optlen)))
	printf("  time: %i\n", optval);
*/
}

static void set_options(JNIEnv *env, int rs)
{
    int val;
    socklen_t vallen;

    val = 1 << 19;
    rs_setsockopt(rs, SOL_SOCKET, SO_SNDBUF, (void *) &val, sizeof val);
    rs_setsockopt(rs, SOL_SOCKET, SO_RCVBUF, (void *) &val, sizeof val);

    val = 1;
    rs_setsockopt(rs, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val));

    // if (flags & MSG_DONTWAIT)
// 	rs_fcntl(rs, F_SETFL, O_NONBLOCK);

    if (USE_RS) {
	val = 0;
	rs_setsockopt(rs, SOL_RDMA, RDMA_INLINE, (void *) &val, sizeof val);
    }


#if TIMEOUT > 0
	struct timeval timeout;      
	timeout.tv_sec = TIMEOUT;
	timeout.tv_usec = 0;

	if (!USE_RS) {
	    if (rs_setsockopt (rs, SOL_SOCKET, SO_RCVTIMEO, (void *)&timeout,
			sizeof(timeout)) < 0) {
		printf("setsockopt failed\n");
		fflush(stdout);
	    }

	    if (rs_setsockopt (rs, SOL_SOCKET, SO_SNDTIMEO, (void *)&timeout,
			sizeof(timeout)) < 0)
		printf("setsockopt failed\n");
		fflush(stdout);
	}
#endif

    if (keepalive)
	set_keepalive(env, rs);
}


int getPeerIP(JNIEnv *env, char *buffer, int len_buf, int sockfd) {
    socklen_t len;
    struct sockaddr_storage addr;
    int port;
    char buf[32];

    len = sizeof addr;
    int ret;
    if ((ret = rs_getpeername(sockfd, (struct sockaddr*)&addr, &len)) < 0) {
	throwException(env, "getpeername", strerror(errno));
	return ret;
    }

    // deal with both IPv4 and IPv6:
    if (addr.ss_family == AF_INET) {
	struct sockaddr_in *s = (struct sockaddr_in *)&addr;
	port = ntohs(s->sin_port);
	inet_ntop(AF_INET, &s->sin_addr, buffer, len_buf);
    } else { // AF_INET6
	struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
	port = ntohs(s->sin6_port);
	inet_ntop(AF_INET6, &s->sin6_addr, buffer, len_buf);
    }
    strcat(buffer, ":");
    sprintf(buf, "%d", port);
    strcat(buffer, buf);

    return 0;
}


int getSockIP(JNIEnv *env, char *buffer, int len_buf, int sockfd) {
    socklen_t len;
    struct sockaddr_storage addr;
    int port;
    char buf[32];

    len = sizeof addr;
    int ret;
    if ((ret = rs_getsockname(sockfd, (struct sockaddr*)&addr, &len)) < 0) {
	throwException(env, "getsockname", strerror(errno));
	return ret;
    }

    // deal with both IPv4 and IPv6:
    if (addr.ss_family == AF_INET) {
	struct sockaddr_in *s = (struct sockaddr_in *)&addr;
	port = ntohs(s->sin_port);
	inet_ntop(AF_INET, &s->sin_addr, buffer, len_buf);
    } else { // AF_INET6
	struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
	port = ntohs(s->sin6_port);
	inet_ntop(AF_INET6, &s->sin6_addr, buffer, len_buf);
    }
    // Unfortunately, this gives 0.0.0.0.
    // So, instead:
    gethostname(buffer, len_buf);
    strcat(buffer, ":");
    sprintf(buf, "%d", port);
    strcat(buffer, buf);

    return 0;
}


int myclientConnect(JNIEnv *env, const char *address, const char *port)
{
#if DEBUG
    puts("myclientConnect");
    fflush(stdout);
#endif
    ai_hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *ai;
    struct pollfd fds;
    int ret, err;
    socklen_t len;

    ret = getaddrinfo(address, port, &ai_hints, &ai);

    if (ret) {
	throwException(env, "getaddrinfo", strerror(errno));
	return ret;
    }

    int sockfd = rs_socket(ai->ai_family, SOCK_STREAM, 0);
    if (sockfd < 0) {
	throwException(env, "rsocket", strerror(errno));
	goto free;
    }

    set_options(env, sockfd);

    ret = rs_connect(sockfd, ai->ai_addr, ai->ai_addrlen);
    if (ret && (errno != EINPROGRESS)) {
	throwException(env, "rconnect", strerror(errno));
	goto close;
    }

    if (ret && (errno == EINPROGRESS)) {
	fds.fd = sockfd;
	fds.events = POLLOUT;
	ret = do_poll(&fds, poll_timeout);
	if (ret)
	    goto close;

	len = sizeof err;
	ret = rs_getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len);
	if (ret)
	    goto close;
	if (err) {
	    ret = -1;
	    throwException(env, "async rconnect", strerror(errno));
	}
    }

close:
    if (ret)
	rs_close(sockfd);
free:
    freeaddrinfo(ai);

    if (ret == 0) {
#if DEBUG
	puts("done myclientConnect");
	fflush(stdout);
#endif
	return sockfd;
    }
    else {
	return ret;
    }
}
double getSecs(struct timeval ts) {
    return ts.tv_sec + (ts.tv_usec / 1e6);
}

void printBW(struct timeval start, struct timeval end, const char *s, 
	int nrBytes) {
    double endD = getSecs(end);
    double startD = getSecs(start);
    double nrMBs = nrBytes / 1024.0 / 1024.0;
    printf("%s: %f MB/s\n", s, nrMBs / (endD - startD));
}

void printTime(struct timeval start, struct timeval end, const char *s) {
    double endD = getSecs(end);
    double startD = getSecs(start);
    printf("%s: %f s\n", s, endD - startD);
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_close2(JNIEnv *env, jclass c,
	jint fd) {
    return rs_close(fd);
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_clientConnect2(JNIEnv *env, jclass c, 
	jstring jaddress, jstring jport) {
    const char *address = (*env)->GetStringUTFChars(env, jaddress, NULL);
    const char *port = (*env)->GetStringUTFChars(env, jport, NULL);
    jint result = myclientConnect(env, address, port);
    (*env)->ReleaseStringUTFChars(env, jaddress, address);
    (*env)->ReleaseStringUTFChars(env, jport, port);
    return result;
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_serverCreate2(JNIEnv *env, jclass c) {
    struct addrinfo *ai = NULL;
    struct addrinfo hints;
    int val, ret;
    int p = 7139;
    char port[32];
    int lrs = 0;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_flags = AI_PASSIVE;

#if DEBUG
    fprintf(stdout, "server create\n");
    fflush(stdout);
#endif
    for (;;p++) {
	sprintf(port, "%d", p);
	if (ai != NULL) {
	    freeaddrinfo(ai);
	}
	ret = getaddrinfo(NULL, port, &hints, &ai);

	if (ret) {
	    throwException(env, "getaddrinfo", gai_strerror(ret));
	    return ret;
	}

	lrs = rs_socket(ai->ai_family, SOCK_STREAM, 0);
	if (lrs < 0) {
	    throwException(env, "rsocket", strerror(errno));
	    ret = lrs;
	    break;
	}

	val = 1;
	ret = rs_setsockopt(lrs, SOL_SOCKET, SO_REUSEADDR, &val, sizeof val);
	if (ret) {
	    throwException(env, "rsetsockopt SO_REUSEADDR", strerror(errno));
	    break;
	}

	ret = rs_bind(lrs, ai->ai_addr, ai->ai_addrlen);
	if (ret) {
	    if (errno == EADDRINUSE) {
		rs_close(lrs);
		continue;
	    }
	    throwException(env, "rbind", strerror(errno));
	    break;
	}

	set_options(env, lrs);

	ret = rs_listen(lrs, SOMAXCONN);
	if (ret) {
	    if (errno == EADDRINUSE) {
		rs_close(lrs);
		continue;
	    }
	    throwException(env, "rlisten", strerror(errno));
	}
	break;
    }

    if (ret && lrs > 0)
	rs_close(lrs);
free:
    freeaddrinfo(ai);

    if (ret) {
	return ret;
    }
#if DEBUG
    puts("done server create");
    fflush(stdout);
#endif
    return lrs;
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_accept2(JNIEnv *env, jclass c, jint l_sockfd) {
    int sockfd;

#if DEBUG
    puts("Accept");
    fflush(stdout);
#endif

    do {
	sockfd = rs_accept(l_sockfd, NULL, 0);
    } while (sockfd < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
    if (sockfd < 0) {
	throwException(env, "raccept", strerror(errno));
	return sockfd;
    }

#if DEBUG
    puts("done Accept");
    fflush(stdout);
#endif
    return sockfd;
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_send2(JNIEnv *env, jclass c, jint sockfd, jobject bb, jint offset, jint size) {
    int retval;
#if TIMING
    struct timeval start;
    struct timeval end;

    gettimeofday(&start, NULL);
#endif
    void *fa = (*env)->GetDirectBufferAddress(env, bb);
#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "getAddress");

    gettimeofday(&start, NULL);
#endif
    retval = send_xfer(env, sockfd, fa + offset, size);
#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "send");
    printBW(start, end, "send", size);
#endif
    return retval;
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_receive2(JNIEnv *env, jclass c, jint sockfd, jobject bb, jint offset, jint size, jboolean fully) {
    int retval;
#if TIMING
    struct timeval start;
    struct timeval end;
    gettimeofday(&start, NULL);
#endif
    void *fa = (*env)->GetDirectBufferAddress(env, bb);

#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "getAddress");

    gettimeofday(&start, NULL);
#endif
    retval = recv_xfer(env, sockfd, bb, size, offset, (int) fully);
#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "receive");
    printBW(start, end, "receive", retval);
#endif
    return retval;
}


#define BUF_SZ 512

JNIEXPORT jstring JNICALL Java_ibis_ipl_impl_ib_IBCommunication_getPeerIP2(JNIEnv * env, jclass c, jint sockfd) {
    char buf[BUF_SZ];
    buf[0] = '\0';

    jstring result;
    if (getPeerIP(env, buf, BUF_SZ, sockfd) < 0) {
	result = (*env)->NewStringUTF(env, "");
    }
    else {
	result = (*env)->NewStringUTF(env, buf);
    }

    return result;
}

JNIEXPORT jstring JNICALL Java_ibis_ipl_impl_ib_IBCommunication_getSockIP2(JNIEnv * env, jclass c, jint sockfd) {
    char buf[BUF_SZ];
    buf[0] = '\0';

    jstring result;
    if (getSockIP(env, buf, BUF_SZ, sockfd) < 0) {
	result = (*env)->NewStringUTF(env, "");
    }
    else {
	result = (*env)->NewStringUTF(env, buf);
    }

    return result;
}

JNIEXPORT void JNICALL Java_ibis_ipl_impl_ib_IBCommunication_initialize(JNIEnv * env, jclass c, jboolean jblocking, jint jmaxsize) {

    maxsize = jmaxsize;
    if (jblocking) {
    } else {
#if TIMEOUT > 0
	throwException(env, "cannot set non-blocking when TIMEOUT is set", strerror(errno));
#endif
    }
}
