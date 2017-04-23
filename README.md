# Hyperquicksort
This repo provides an implemenation of hyperquicksort, a parallel sorting algorithm using MPI.

##compilation script##
mpicc -g -Wall -o sort hyperquicksort.c -lm

##execution script##
mpiexec -n num_procs ./sort inputfile outputfile
