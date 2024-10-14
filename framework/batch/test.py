from mpi4py import MPI
from batch_utils import BatchCreator


comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    bc = BatchCreator("./project.json")
    bc.create_ranks()
    rank0 = bc.ranks[0]
    comm.send(rank0[1], 1, 11)
else:
    data = comm.recv(source=0)
    print(data)
