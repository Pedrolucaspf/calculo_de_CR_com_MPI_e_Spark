from mpi4py import MPI
import random
import math
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--alunos", type=int)
parser.add_argument("--disciplinas", type=int)

args = parser.parse_args()

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
        
        list_notas = []
        for j in range(num_disc):
            min_nota = 6.0
            aprov = random.randint(0, 2)
            if(aprov == 0):
                min_nota = 0.0
            nota = round(random.uniform(min_nota, 10.0), 1)
            if(nota >= 4.0 and nota < 6.0):
                vs = round(random.uniform(0.0, 10.0), 1)
                nota = round((nota+vs)/2.0, 1)
            horas = random.randint(1, 3) * 32
            registro = (nota, horas)
            list_notas.append(registro)

        total_horas = 0
        peso_disc = 0
        for j in range(num_disc):
            peso_disc += list_notas[j][0] * list_notas[j][1]
            total_horas += list_notas[j][1]
        
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
