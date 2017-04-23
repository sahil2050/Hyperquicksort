/* Parallel Programming Lab3
*   Topic : Hyperquicksort
*   Author: Sahil Aggarwal
*   Copyright 2017 @ SahilAggarwal
*/
#include<stdio.h>
#include<math.h>
#include<stdlib.h>
#include<mpi.h>
#include<string.h>

int cmpfunc (const void * a, const void * b){
   return  *(int*)a >= *(int*)b ? 1 : -1;
}

int main(int argc, char** argv){
    int n, local_n, comm_sz, world_rank, logp, median, pivot;
    int *A, *local_A, *recv_buf, *merge_buf;
    double start,end;
    MPI_Status status;

    if(argc < 3){
        printf("Input and output files not provided\n");
        return 0;
    }
    
    MPI_Init(NULL,NULL);
    MPI_Comm_size(MPI_COMM_WORLD,&comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD,&world_rank);

    logp = round(log(comm_sz)/log(2));
    // read input
    if(world_rank == 0){
        char* filename = argv[1];
        FILE *file = fopen(filename,"r");
        //printf("%s\n",filename);
        if (file) {
	        fscanf(file,"%d",&n);
            A = (int*) malloc(sizeof(int)*n);
            for(int i = 0; i < n; i++)
                fscanf(file,"%d",&A[i]);
        }
        fclose(file);
    }
    // distribute the input data
    MPI_Bcast(&n,1,MPI_INT,0,MPI_COMM_WORLD);
    local_A = (int*) malloc(sizeof(int)*n);
    recv_buf = (int*) malloc(sizeof(int)*n);
    merge_buf = (int*) malloc(sizeof(int)*n);
    local_n = n/comm_sz;
    MPI_Scatter(A,local_n,MPI_INT,local_A,local_n,MPI_INT,0,MPI_COMM_WORLD);

    start= MPI_Wtime();
    // sort each local array
    qsort(local_A,local_n,sizeof(int),cmpfunc);

    //MPI_Comm old_comm = MPI_COMM_WORLD;
    for(int iter = 0; iter < logp; iter++){
        //printf("iter : %d\n",iter);
        // create communicator based on no of iteration
        int color = pow(2,iter)*world_rank/comm_sz;
        MPI_Comm new_comm;
        MPI_Comm_split(MPI_COMM_WORLD, color, world_rank, &new_comm);
        int rank,sz;
        MPI_Comm_rank(new_comm,&rank);
        MPI_Comm_size(new_comm,&sz);

        // process 0 will broadcast its median
        median = local_A[local_n/2];
        MPI_Bcast(&median, 1, MPI_INT, 0, new_comm);
        //printf("median : %d\n",median);
        // each process in upper half will swap its low list with high list of corresponding lower half
        pivot = 0;
        while(pivot < local_n && local_A[pivot] < median)
            pivot++;

        int pair_process = (rank+(sz>>1))%sz;
        
        if(rank >= (sz>>1)){ // in upper half
            MPI_Send(local_A,pivot,MPI_INT,pair_process,1,new_comm);    
            MPI_Recv(recv_buf,n,MPI_INT,pair_process,1,new_comm,&status);
        }else{
            MPI_Recv(recv_buf,n,MPI_INT,pair_process,1,new_comm,&status);
            MPI_Send(local_A+pivot,local_n - pivot,MPI_INT,pair_process,1,new_comm);
        }

        //printf("swapped iter: %d rank: %d\n",iter,world_rank);
        // merge the two sorted lists
        int recv_count;
            MPI_Get_count(&status,MPI_INT,&recv_count);
        
        int i,j = 0,k = 0, i_end;
        if(rank >= (sz>>1)){
            i = pivot, i_end = local_n;
            local_n = local_n - pivot + recv_count;
        }else{
            i = 0, i_end = pivot;
            local_n = pivot + recv_count;
        }

        while(i < i_end && j < recv_count){
            if(local_A[i] < recv_buf[j])
                merge_buf[k++] = local_A[i++];
            else
                merge_buf[k++] = recv_buf[j++];
        }
        while(i < i_end)
            merge_buf[k++] = local_A[i++];
        while(j < recv_count)
            merge_buf[k++] = recv_buf[j++];
    
        // copy merge buf into local_A
        for(int i = 0; i < local_n; i++)
            local_A[i] = merge_buf[i];
        
        //printf("merged iter: %d rank: %d\n",iter,world_rank);

        MPI_Comm_free(&new_comm);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    end = MPI_Wtime();
    // print the output
    if(world_rank != 0)
        MPI_Send(local_A,local_n,MPI_INT,0,1,MPI_COMM_WORLD);

    //printf("sent %d\n",world_rank);

    if(world_rank == 0){
        int recv_count = local_n;
        for(int i = 1; i < comm_sz; i++){
            MPI_Recv(local_A + recv_count,n,MPI_INT,i,1,MPI_COMM_WORLD,&status);
            int temp_count;
            MPI_Get_count(&status,MPI_INT,&temp_count);
            recv_count += temp_count;
            //printf("recieved from %d\n",i);
        }
        char* filename = argv[2];
        FILE *file = fopen(filename,"w");
        if (file) {        
            for(int i = 0 ; i < n; i++)
                fprintf(file,"%d\n",local_A[i]);
            for(int i = 1 ; i < n; i++)
                if(local_A[i-1] > local_A[i])
                    printf("ERROR: %d\n",i);
        }
        fclose(file);

        printf("time taken: %lf\n",end-start);
    }

    MPI_Finalize();
}