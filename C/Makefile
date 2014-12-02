CC		= gcc
CFLAGS		= -fPIC -I../include/infiniband -I../include/rdma
LFLAGS		= -shared -o $(SONAME)
AR		= ar
ARFLAGS		= cru
RANLIB		= ranlib
LIBS		= ../lib/librdmacm.a
SRC		= ibcomm.c common.c
OBJ		= $(SRC:.c=.o)
SONAME		= libibcomm.so
LIBNAME		= libibcomm.a

DEPFILE		= .depends
DEPFLAGS	= -Y -f$(DEPFILE)

all:	$(SONAME) $(LIBNAME)

$(SONAME): $(DEPFILE) $(OBJ)
	$(CC) $(LFLAGS) -o $@ $(OBJ) $(LIBS) -libverbs -lpthread

$(LIBNAME): $(DEPFILE) $(OBJ)
	$(AR) $(ARFLAGS) $(LIBNAME) $(OBJ)
	$(RANLIB) $(LIBNAME)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

$(DEPFILE):
	touch $(DEPFILE)
	makedepend $(DEPFLAGS) $(SRC) $(HDR)


depend:
	rm -rf $(DEPFILE)
	make $(DEPFILE)


clean: 
	rm -f $(SONAME) $(LIBNAME) *.o .depends .depends.bak

