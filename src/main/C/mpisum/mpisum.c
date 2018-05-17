#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

#define N 1E8
#define d 1E-8

int main (int argc, char* argv[])
{
    int rank, size, record, result=0, sum=0;
    double begin=0.0, end=0.0;
    FILE *fpin, *fpout;

    MPI_Init (&argc, &argv);
    
    //Get process ID
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);
    
    //Get processes Number
    MPI_Comm_size (MPI_COMM_WORLD, &size);
    
    //Synchronize all processes and get the begin time
    MPI_Barrier(MPI_COMM_WORLD);
    begin = MPI_Wtime();
    
    srand((int)time(0));
    
    char* input_file_path = argv[1];
    char* output_file_path = argv[2];
    
    if((fpin=fopen(input_file_path,"r"))<0)
    {
     printf("open input file fail!\n");
     exit(-1);
    }
    while (fscanf(fpin, "%d", &record) > 0) {
      //printf("record: %d\n", record);
      result += record;
    }
    fclose(fpin);
    //printf("result: %d\n", result);
 
    //Sum up all results
    MPI_Reduce(&result, &sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    //printf("rank%d reduce done\n", rank);

    //Synchronize all processes and get the end time
    MPI_Barrier(MPI_COMM_WORLD);
    end = MPI_Wtime();
    
    //Caculate and print PI
    if (rank==0)
    {
        if((fpout=fopen(output_file_path, "w"))<0)
        {
          printf("open output file fail!\n");
          exit(-1);
        }
        printf("np=%2d;    Time=%fs;    sum=%d\n", size, end-begin, sum);
        fclose(fpout);
    }
    
    MPI_Finalize();
    
    return 0;
}

