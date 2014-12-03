#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
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

#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>
#include "common.h"

#define BLOCKING 0
#define TIMEOUT 0
#define DEBUG 0
#define MAXSIZE 0

static int isListening = 0;
static int l_sockfd = -1;

#if BLOCKING
// the message size has to equal the buffer size, otherwise it would block
// No, not if flags == 0. --Ceriel
static int flags = 0;
#else
static int flags = MSG_DONTWAIT;
#endif

static int poll_timeout = 0;
static char *port = "0";
static int keepalive = 0;
static struct addrinfo ai_hints;
static int countm1Sent = 0;
static int countm1Received = 0;



int getPeerIP(char *buf, int buf_len, int sockfd);


static void printTime(int secBefore) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    tv.tv_sec -= secBefore;
    struct tm tm = *localtime(&tv.tv_sec);

    printf("%02d:%02d:%02d-%02d", tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec / 10000);
}


static void printIP(char * prefix, int sockfd) {
    char ipstr[INET6_ADDRSTRLEN];
    ipstr[0] = '\0';
    getPeerIP(ipstr, INET6_ADDRSTRLEN, sockfd);
    printf("%s%s\n", prefix, ipstr);
}


static int send_xfer(int sockfd, void *buf, int size)
{
    int offset, ret;


    for (offset = 0; offset < size; ) {
	int sz = size - offset;
#if MAXSIZE > 0
	if (sz > MAXSIZE) sz = MAXSIZE;
#endif
	ret = rs_send(sockfd, buf + offset, sz, flags);
#if DEBUG
	if (ret != -1) {
	    countm1Sent = 0;
	    printf("rs_send(%d, 0x%x, %d) = %d (%d to go)\n", sockfd, buf + offset, sz, ret, size - offset);
	    fflush(stdout);
	} else {
	    countm1Sent++;
	    if (countm1Sent == 10000) {
		printf("%d times rs_send(%d, 0x%x, %d) = %d\n", countm1Sent, sockfd, buf + offset, sz, ret);
		fflush(stdout);
		countm1Sent = 0;
	    }
	}
#endif
	if (ret > 0) {
	    offset += ret;
	} else if (ret < 0) {
	    if (errno == EWOULDBLOCK || errno == EAGAIN) {
#if BLOCKING && TIMEOUT > 0
		printf("timeout at ");
		printTime(TIMEOUT);
		printf(" rs_send(), still need to send %d bytes to sockfd %d ", size - offset, sockfd);
		printIP("", sockfd);
		printf("\n");
		fflush(stdout);
#endif
	    }
	    else {
		perror("rsend");
		return ret;
	    }
	}
	else { // ret == 0
	    printf("SHIT!: send returns 0 and no error is set...\n"); 
	    fflush(stdout);
	    return -1;
	}
    }

    return 0;
}

static int recv_xfer(int sockfd, void *buf, int size)
{
    int offset, ret;

    for (offset = 0; offset < size; ) {
	int sz = size - offset;
#if MAXSIZE > 0
	if (sz > MAXSIZE) sz = MAXSIZE;
#endif
	ret = rs_recv(sockfd, buf + offset, sz, flags);
#if DEBUG
	if (ret != -1) {
	    countm1Received = 0;
	    printf("rs_recv(%d, 0x%x, %d) = %d (%d to go)\n", sockfd, buf + offset, sz, ret, size - offset);
	    fflush(stdout);
	} else {
	    countm1Received++;
	    if (countm1Received == 10000) {
		printf("%d times rs_recv(%d, 0x%x, %d) = %d\n", countm1Received, sockfd, buf + offset, sz, ret);
		fflush(stdout);
		countm1Received = 0;
	    }
	}
#endif
	if (ret > 0) {
	    offset += ret;
	} 
	else if (ret < 0) {
	    if (errno == EWOULDBLOCK || errno == EAGAIN) {
#if BLOCKING && TIMEOUT > 0
		printf("timeout at ");
		printTime(TIMEOUT);
		printf(" rs_recv(), still expecting %d bytes from sockfd %d ", size - offset, sockfd);
		printIP("", sockfd);
		printf("\n");
		fflush(stdout);
#endif
	    }
	    else {
		perror("rrecv");
		return ret;
	    }
	}
	else { // ret == 0
	    printf("SHIT!: there are no longer messages available on the other side, but I still need %d bytes\n", size - offset); 
	    fflush(stdout);
	    return -1;
	}
    }

    return 0;
}


static int sync_test(int sockfd,  char *address)
{
    int ret;
    int buf[16];

    ret = address ? send_xfer(sockfd, (void *) buf, 16) : 
	recv_xfer(sockfd, (void *) buf, 16);
    if (ret)
	return ret;

    return address ? recv_xfer(sockfd, (void *) buf, 16) : 
	send_xfer(sockfd, (void *) buf, 16);
}


int mysend(int sockfd, void *buf, size_t size) {
    //printIP("sending to: ", sockfd);
    //sync_test(sockfd, "a");
    int i = send_xfer(sockfd, buf, size);
    return i;
}

int myreceive(int sockfd, void *buf, size_t size) {
    //printIP("receiving from: ", sockfd);
    //sync_test(sockfd, NULL);
    int i = recv_xfer(sockfd, buf, size);

    return i;
}


static void set_keepalive(int rs)
{
    int optval;
    socklen_t optlen = sizeof(optlen);

    optval = 1;
    if (rs_setsockopt(rs, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen)) {
	perror("rsetsockopt SO_KEEPALIVE");
	return;
    }

    optval = keepalive;
    if (rs_setsockopt(rs, IPPROTO_TCP, TCP_KEEPIDLE, &optval, optlen))
	perror("rsetsockopt TCP_KEEPIDLE");

    if (!(rs_getsockopt(rs, SOL_SOCKET, SO_KEEPALIVE, &optval, &optlen)))
	printf("Keepalive: %s\n", (optval ? "ON" : "OFF"));

    if (!(rs_getsockopt(rs, IPPROTO_TCP, TCP_KEEPIDLE, &optval, &optlen)))
	printf("  time: %i\n", optval);
}

static void set_options(int rs)
{
    int val;

    val = 1 << 19;
    rs_setsockopt(rs, SOL_SOCKET, SO_SNDBUF, (void *) &val, sizeof val);
    rs_setsockopt(rs, SOL_SOCKET, SO_RCVBUF, (void *) &val, sizeof val);

    val = 1;
    rs_setsockopt(rs, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val));

    if (flags & MSG_DONTWAIT)
	rs_fcntl(rs, F_SETFL, O_NONBLOCK);

    if (use_rs) {
	val = 0;
	rs_setsockopt(rs, SOL_RDMA, RDMA_INLINE, &val, sizeof val);
    }


#if BLOCKING && TIMEOUT > 0
    struct timeval timeout;      
    timeout.tv_sec = TIMEOUT;
    timeout.tv_usec = 0;

    if (!use_rs) {
	if (rs_setsockopt (rs, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,
		    sizeof(timeout)) < 0) {
	    printf("setsockopt failed\n");
	    fflush(stdout);
	}

	if (rs_setsockopt (rs, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout,
		    sizeof(timeout)) < 0)
	    printf("setsockopt failed\n");
	    fflush(stdout);
    }
#endif

    if (keepalive)
	set_keepalive(rs);
}

static int server_listen(void)
{
    struct addrinfo *ai;
    struct addrinfo hints;
    int val, ret;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_PASSIVE;

#if DEBUG
    puts("server_listen");
    fflush(stdout);
#endif
    ret = getaddrinfo(NULL, port, &hints, &ai);

    if (ret) {
	perror("getaddrinfo");
	return ret;
    }

    int lrs = rs_socket(ai->ai_family, SOCK_STREAM, 0);
    if (lrs < 0) {
	perror("rsocket");
	ret = lrs;
	goto free;
    }

    val = 1;
    ret = rs_setsockopt(lrs, SOL_SOCKET, SO_REUSEADDR, &val, sizeof val);
    if (ret) {
	perror("rsetsockopt SO_REUSEADDR");
	goto close;
    }

    ret = rs_bind(lrs, ai->ai_addr, ai->ai_addrlen);
    if (ret) {
	perror("rbind");
	goto close;
    }

    ret = rs_listen(lrs, SOMAXCONN);
    if (ret)
	perror("rlisten");

close:
    if (ret)
	rs_close(lrs);
free:
    freeaddrinfo(ai);

    if (ret) {
	return ret;
    }
    else {
#if DEBUG
	puts("done server_listen");
	fflush(stdout);
#endif
	return lrs;
    }
}

static int myAccept(int l_sockfd)
{
    int sockfd;

#if DEBUG
    puts("myAccept");
    fflush(stdout);
#endif

    do {
	sockfd = rs_accept(l_sockfd, NULL, 0);
    } while (sockfd < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
    if (sockfd < 0) {
	perror("raccept");
	return sockfd;
    }

    set_options(sockfd);
#if DEBUG
    puts("done myAccept");
    fflush(stdout);
#endif
    return sockfd;
}


// Multiple threads are not allowed to enter this. 
int myserverConnect() {
#if DEBUG
    puts("myserverConnect");
    fflush(stdout);
#endif

    int sockfd;
    ai_hints.ai_socktype = SOCK_STREAM;

    if (!isListening) {
	if ((l_sockfd = server_listen()) < 0) {
	    return l_sockfd;
	}
	isListening = 1;
	// move this to where it came from?
	set_options(l_sockfd);
    }

    if ((sockfd = myAccept(l_sockfd)) < 0) {
	return sockfd;
    }

#if DEBUG
    printf("will receive on sockfd %d from ", sockfd);
    printIP("", sockfd);
    fflush(stdout);
#endif

    int ret;
    if ((ret = sync_test(sockfd, NULL)) < 0) {
	return ret;
    }

#if DEBUG
    puts("done myserverConnect");
    fflush(stdout);
#endif
    return sockfd;
}


int getPeerIP(char *buffer, int len_buf, int sockfd) {
    socklen_t len;
    struct sockaddr_storage addr;
    int port;

    len = sizeof addr;
    int ret;
    if ((ret = rs_getpeername(sockfd, (struct sockaddr*)&addr, &len)) < 0) {
	perror("getpeername");
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

    return 0;
}


int myclientConnect(char *address)
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
	perror("getaddrinfo");
	return ret;
    }

    int sockfd = rs_socket(ai->ai_family, SOCK_STREAM, 0);
    if (sockfd < 0) {
	perror("rsocket");
	goto free;
    }

    set_options(sockfd);

    ret = rs_connect(sockfd, ai->ai_addr, ai->ai_addrlen);
    if (ret && (errno != EINPROGRESS)) {
	perror("rconnect");
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
	    errno = err;
	    perror("async rconnect");
	}
    }

close:
    if (ret)
	rs_close(sockfd);
free:
    freeaddrinfo(ai);

    if (ret == 0) {
	if ((ret = sync_test(sockfd, address)) < 0) {
	    return ret;
	}
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
