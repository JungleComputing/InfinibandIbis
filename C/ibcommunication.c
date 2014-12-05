#include <jni.h>
#include <stdio.h>

#include <sys/time.h>
#include "common.h"
#include "ibcommunication.h"

#define TIMING 0

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
	jstring address, jstring port) {
    const char *astr = (*env)->GetStringUTFChars(env, address, NULL);
    const char *pstr = (*env)->GetStringUTFChars(env, port, NULL);
    jint result = myclientConnect(astr, pstr);
    (*env)->ReleaseStringUTFChars(env, address, astr);
    (*env)->ReleaseStringUTFChars(env, port, pstr);
    return result;
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_serverCreate2(JNIEnv *env, jclass c) {
    return server_listen();
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_accept2(JNIEnv *env, jclass c, jint fd) {
    jint result = myAccept(fd);
    return result;
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
    retval = mysend(sockfd, fa + offset, size);
#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "send");
    printBW(start, end, "send", size);
#endif
    return retval;
}

JNIEXPORT jint JNICALL Java_ibis_ipl_impl_ib_IBCommunication_receive2(JNIEnv *env, jclass c, jint sockfd, jobject bb, jint offset, jint size) {
    int retval;
#if TIMING
    struct timeval start;
    struct timeval end;
    gettimeofday(&start, NULL);
#endif
    void *fa = (*env)->GetDirectBufferAddress(env, bb);
    if (fa == NULL) {
	fprintf(stderr, "Oeps: GetDirectBufferAddress gives null!, bb = %p\n", bb);
    }

#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "getAddress");

    gettimeofday(&start, NULL);
#endif
    retval = myreceive(sockfd, fa + offset, size);
#if TIMING
    gettimeofday(&end, NULL);
    printTime(start, end, "receive");
    printBW(start, end, "receive", size);
#endif
    return retval;
}


#define BUF_SZ 512

JNIEXPORT jstring JNICALL Java_ibis_ipl_impl_ib_IBCommunication_getPeerIP2(JNIEnv * env, jclass c, jint sockfd) {
    char buf[BUF_SZ];
    buf[0] = '\0';

    jstring result;
    if (getPeerIP(buf, BUF_SZ, sockfd) < 0) {
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
    if (getSockIP(buf, BUF_SZ, sockfd) < 0) {
	result = (*env)->NewStringUTF(env, "");
    }
    else {
	result = (*env)->NewStringUTF(env, buf);
    }

    return result;
}
