from mpi4py import MPI
import numpy as np
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--alunos", type=int)
parser.add_argument("--disciplinas", type=int)

args = parser.parse_args()

rng = np.random.default_rng()

def main():
    max_tempo = 60.0
    tempo_excedido = 0    
    start_time = MPI.Wtime()

    comm = MPI.COMM_WORLD
    my_rank = comm.Get_rank()
    comm_sz = comm.Get_size()

    if(my_rank == 0):
        input_start_time = MPI.Wtime()

        if args.alunos:
            num_alunos = args.alunos
        else:
            num_alunos = int(input("Insira a quantidade de alunos:"))
    
        if args.disciplinas:
            num_disc = args.disciplinas
        else:
            num_disc = int(input("Insira a quantidade de disciplinas:"))

        input_end_time = MPI.Wtime()
        alunos_por_proc = num_alunos // comm_sz
        resto = num_alunos % comm_sz

        aprov_array = rng.integers(0, 2, size=num_alunos)
        list_notas = []
        list_horas = []

        for i in range(num_alunos):
            current_time = MPI.Wtime()
            input_time = input_end_time - input_start_time
            time_passed = current_time - start_time - input_time
            if (time_passed >= max_tempo):
                tempo_excedido = 1
                break

            notas_array = np.empty(num_disc, dtype=np.float64)
            for j in range(num_disc):
                min_nota = 6.0
                if(aprov_array[i] == 0):
                    min_nota = 0.0

                nota = rng.uniform(min_nota, 10.0)
                if(nota >= 4.0 and nota < 6.0):
                    vs = rng.uniform(0.0, 10.0)
                    nota = (nota+vs)/2.0
                notas_array[j] = nota
            
            list_notas.append(notas_array)
        
            h_range = rng.integers(1, 4, size=num_disc)
            horas_array = h_range*32 
            list_horas.append(horas_array)

        if (tempo_excedido == 0):
            local_array_notas = np.concatenate(list_notas)
            local_array_horas = np.concatenate(list_horas)

            local_array_notas = local_array_notas.ravel()
            local_array_horas = local_array_horas.ravel()

            sendcounts = np.concatenate((np.full(resto, (alunos_por_proc+1)*num_disc), np.full(comm_sz-resto, (alunos_por_proc)*num_disc)))
            displacements = np.concatenate((np.arange(0, resto*(alunos_por_proc+1)*num_disc, (alunos_por_proc+1)*num_disc), np.arange(resto*(alunos_por_proc+1)*num_disc, num_alunos*num_disc, alunos_por_proc*num_disc)))
    
    else:
        local_array_horas = None
        local_array_notas = None
        input_time = None
        sendcounts = None
        displacements = None
        num_disc = None

    input_time = comm.bcast(input_time, root=0)
    tempo_excedido = comm.bcast(tempo_excedido, root=0)

    if(tempo_excedido == 0):
        current_time = MPI.Wtime()
        time_passed = current_time - start_time - input_time
        if (time_passed >= max_tempo):
            tempo_excedido = 1
        else:
            num_disc = comm.bcast(num_disc, root=0)
            sendcounts = comm.bcast(sendcounts, root=0)
            displacements = comm.bcast(displacements, root=0)

            array_notas = np.empty(sendcounts[my_rank], dtype=np.float64)
            array_horas = np.empty(sendcounts[my_rank], dtype=np.int32)

            comm.Scatterv([local_array_notas, sendcounts, displacements, MPI.DOUBLE], array_notas, root=0)
            comm.Scatterv([local_array_horas, sendcounts, displacements, MPI.INT32_T], array_horas, root=0)

            local_num_alunos = sendcounts[my_rank] // num_disc

            array_notas = array_notas.reshape(local_num_alunos, num_disc)
            array_horas = array_horas.reshape(local_num_alunos, num_disc)

            array_peso_disc = np.sum(array_notas * array_horas, axis=1)
            array_total_horas = np.sum(array_horas, axis=1)

            local_array_cr = np.round(array_peso_disc / array_total_horas, 1)

    if(tempo_excedido == 0):
        current_time = MPI.Wtime()
        time_passed = current_time - start_time - input_time
        if (time_passed < max_tempo):
            # Usa gather (pickle) ao invés de Gatherv (binário) — Alteração 2
            gathered_cr = comm.gather(local_array_cr, root=0)

    if(my_rank == 0):
        end_time = MPI.Wtime()
        total_time = end_time - start_time
        time_passed = total_time - input_time
        if (time_passed >= max_tempo):
            print(f"Tempo de processamento máximo ({max_tempo} segundos) excedido")
        else:
            print_start_time = MPI.Wtime()
            # Concatena os arrays recebidos via gather
            array_cr = np.concatenate(gathered_cr)
            num_turmas = int(np.ceil(num_alunos / 20))
            for i in range(num_turmas):
                print("----------------------------------")
                print(f"Turma {i+1}:")
                for j in range(i*20, (i+1)*20):
                    if(j < array_cr.size):
                        print(f"Aluno: {j+1} - CR: {array_cr[j]}")
                    else:
                        break
            
            print("----------------------------------")
            print_end_time = MPI.Wtime()
            print_time = print_end_time - print_start_time
            print(f"Tempo de processamento: {(time_passed)} segundos")
            print(f"OBS: + {print_time} segundos (impressão)")

if __name__ == "__main__":
    main()
