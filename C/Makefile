CC		= gcc
INCLUDES	= -I../build/include -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
CFLAGS		= -O -fPIC $(INCLUDES)
LFLAGS		= -shared -o $(SONAME)
LIBS		= lib/librdmacm.a
SRC		= ibcommunication.c
OBJ		= $(SRC:.c=.o)
SONAME		= ../build/classes/libibcommunication.so

DEPFILE		= .depends
DEPFLAGS	= -Y -f$(DEPFILE) $(INCLUDES)

all:	$(SONAME)

$(SONAME): $(DEPFILE) $(OBJ)
	$(CC) $(LFLAGS) -o $@ $(OBJ) $(LIBS) -libverbs -lpthread

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

# DO NOT DELETE
