    struct { int a; double b; } value;
    MPI_Datatype mystruct;
    int          blocklens[2];
    MPI_Aint     indices[2];
    MPI_Datatype old_types[2];

    /* One value of each type */
    blocklens[0] = 1;
    blocklens[1] = 1;
    /* The base types */
    old_types[0] = MPI_INT;
    old_types[1] = MPI_DOUBLE;
    /* The locations of each element */
    MPI_Address( &value.a, &indices[0] );
    MPI_Address( &value.b, &indices[1] );
    /* Make relative */
    indices[1] = indices[1] - indices[0];
    indices[0] = 0;
    MPI_Type_struct( 2, blocklens, indices, old_types, &mystruct );
    MPI_Type_commit( &mystruct );

    if (mpiRank == 0) 
    {
        value.a = 1;
        value.b = 2;
    }

    printf("Before BCast - Process %d(%s)\n", mpiRank, argv[0]);

    MPI_Bcast(&value, 1, mystruct, 0, MPI_COMM_WORLD);
	
    printf("After BCase - Process %d(%s) got %d and %lf\n", mpiRank, argv[0], value.a, value.b);
    MilliSleep(2000);

    /* Clean up the type */
    MPI_Type_free( &mystruct );

