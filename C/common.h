/*
 * Copyright (c) 2005-2012 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * $Id$
 */

#include <stdlib.h>
#include <sys/types.h>
#include <byteswap.h>
#include <poll.h>

#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>
#include <infiniband/ib.h>

#if __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t cpu_to_be64(uint64_t x) { return x; }
static inline uint32_t cpu_to_be32(uint32_t x) { return x; }
#else
static inline uint64_t cpu_to_be64(uint64_t x) { return bswap_64(x); }
static inline uint32_t cpu_to_be32(uint32_t x) { return bswap_32(x); }
#endif

extern int use_rs;

#define rs_socket(f,t,p)  use_rs ? rsocket(f,t,p)  : socket(f,t,p)
#define rs_bind(s,a,l)    use_rs ? rbind(s,a,l)    : bind(s,a,l)
#define rs_listen(s,b)    use_rs ? rlisten(s,b)    : listen(s,b)
#define rs_connect(s,a,l) use_rs ? rconnect(s,a,l) : connect(s,a,l)
#define rs_accept(s,a,l)  use_rs ? raccept(s,a,l)  : accept(s,a,l)
#define rs_shutdown(s,h)  use_rs ? rshutdown(s,h)  : shutdown(s,h)
#define rs_close(s)       use_rs ? rclose(s)       : close(s)
#define rs_recv(s,b,l,f)  use_rs ? rrecv(s,b,l,f)  : recv(s,b,l,f)
#define rs_send(s,b,l,f)  use_rs ? rsend(s,b,l,f)  : send(s,b,l,f)
#define rs_recvfrom(s,b,l,f,a,al) \
	use_rs ? rrecvfrom(s,b,l,f,a,al) : recvfrom(s,b,l,f,a,al)
#define rs_sendto(s,b,l,f,a,al) \
	use_rs ? rsendto(s,b,l,f,a,al)   : sendto(s,b,l,f,a,al)
#define rs_poll(f,n,t)	  use_rs ? rpoll(f,n,t)	   : poll(f,n,t)
#define rs_fcntl(s,c,p)   use_rs ? rfcntl(s,c,p)   : fcntl(s,c,p)
#define rs_setsockopt(s,l,n,v,ol) \
	use_rs ? rsetsockopt(s,l,n,v,ol) : setsockopt(s,l,n,v,ol)
#define rs_getsockopt(s,l,n,v,ol) \
	use_rs ? rgetsockopt(s,l,n,v,ol) : getsockopt(s,l,n,v,ol)
#define rs_getpeername(s,a,l) \
	use_rs ? rgetpeername(s,a,l) : getpeername(s,a,l)
#define rs_getsockname(s,a,l) \
	use_rs ? rgetsockname(s,a,l) : getsockname(s,a,l)

int do_poll(struct pollfd *fds, int timeout);
