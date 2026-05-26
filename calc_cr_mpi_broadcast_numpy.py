from mpi4py import MPI
import numpy as np
import math
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
        input_time = input_end_time - input_start_time
    else:
        num_alunos = None
        num_disc = None
        input_time = None
    
    input_time = comm.bcast(input_time, root=0)
    num_alunos = comm.bcast(num_alunos, root=0)
    num_disc = comm.bcast(num_disc, root=0)
    
    alunos_por_proc = num_alunos // comm_sz
    resto = num_alunos % comm_sz
    
    if(my_rank < resto):
        local_range_i = (alunos_por_proc+1)*my_rank
        local_range_f = local_range_i + (alunos_por_proc+1)
    else:
        local_range_i = (alunos_por_proc*my_rank)+resto
        local_range_f = local_range_i + alunos_por_proc

    local_list_cr = []
    for i in range(local_range_i, local_range_f):
        current_time = MPI.Wtime()
        time_passed = current_time - start_time - input_time
        if (time_passed >= max_tempo):
            tempo_excedido = 1
            break
        
        # Gera notas e horas usando NumPy vetorizado (Alteração 1)
        aprov_array = rng.integers(0, 3, size=num_disc)
        min_nota = np.where(aprov_array == 0, 0.0, 6.0)
        array_notas = rng.uniform(min_nota, 10.0, size=num_disc)
        
        # Aplica VS (substitutiva) para notas entre 4.0 e 6.0
        vs_mask = (array_notas >= 4.0) & (array_notas < 6.0)
        if np.any(vs_mask):
            vs_array = rng.uniform(0.0, 10.0, size=num_disc)
            array_notas = np.where(vs_mask, (array_notas + vs_array) / 2.0, array_notas)
        
        array_notas = np.round(array_notas, 1)
        
        h_range = rng.integers(1, 4, size=num_disc)
        array_horas = h_range * 32

        # Cálculo vetorizado do CR
        peso_disc = np.sum(array_notas * array_horas)
        total_horas = np.sum(array_horas)
        
        cr = round(peso_disc / total_horas, 1)
        local_list_cr.append(cr)

    if(tempo_excedido == 0):
        current_time = MPI.Wtime()
        time_passed = current_time - start_time - input_time
        if (time_passed < max_tempo):
            list_cr = comm.gather(local_list_cr, root=0)

    
    if(my_rank == 0):
        end_time = MPI.Wtime()
        input_time = input_end_time - input_start_time
        total_time = end_time - start_time
        time_passed = total_time - input_time
        if (time_passed >= max_tempo):
            print(f"Tempo de processamento máximo ({max_tempo} segundos) excedido")
        else:
            print_start_time = MPI.Wtime()
            list_cr = [item for sublist in list_cr for item in sublist]
            num_turmas = math.ceil(num_alunos / 20)
            for i in range(num_turmas):
                print("----------------------------------")
                print(f"Turma {i+1}:")
                for j in range(i*20, (i+1)*20):
                    if(j < len(list_cr)):
                        print(f"Aluno {j+1} - CR: {list_cr[j]}")
                    else:
                        break
            
                
            print("----------------------------------")
            print_end_time = MPI.Wtime()
            print_time = print_end_time - print_start_time
            print(f"Tempo de processamento: {(time_passed)} segundos")
            print(f"OBS: + {print_time} segundos (impressão)")

if __name__ == "__main__":
    main()
