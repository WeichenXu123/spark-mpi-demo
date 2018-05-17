EXECS=mpisum
MPICC?=mpicc

all: ${EXECS}

mpisum: mpisum.c
	${MPICC} -o mpisum mpisum.c

clean:
	rm -f ${EXECS}
