from pyspark.sql import SparkSession
import numpy as np
import time
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--alunos", type=int)
parser.add_argument("--disciplinas", type=int)
parser.add_argument("-np", "--numproc", type=int, default=12)

args = parser.parse_args()
PARTICOES = args.numproc

max_tempo = 60.0
tempo_excedido = 0  

def calc_cr(aluno):
    global tempo_excedido
    current_time = time.perf_counter()
    time_passed = current_time - start_time - input_time
    if (time_passed >= max_tempo):
        tempo_excedido = 1
        return (-1, -1.0)

    rng = np.random.default_rng()

    aprov_array = rng.integers(0, 2, size=num_disc)
    array_notas = np.empty(num_disc, dtype=np.float64)
    for i in range(num_disc):
        min_nota = 6.0
        if(aprov_array[i] == 0):
            min_nota = 0.0

        nota = rng.uniform(min_nota, 10.0)
        if(nota >= 4.0 and nota < 6.0):
            vs = rng.uniform(0.0, 10.0)
            nota = (nota+vs)/2.0
        array_notas[i] = nota
        
    h_range = rng.integers(1, 4, size=num_disc)
    array_horas = h_range*32 

    peso_disc = np.sum(array_notas * array_horas)
    total_horas = np.sum(array_horas)
        
    cr = round(peso_disc / total_horas, 1)

    return (aluno+1, cr)    


spark = SparkSession.builder.appName("Calc_CR") .config("spark.default.parallelism", PARTICOES).getOrCreate()

sc = spark.sparkContext

start_time = time.perf_counter()

input_start_time = time.perf_counter()

if args.alunos:
    num_alunos = args.alunos
else:
    num_alunos = int(input("Insira a quantidade de alunos:"))

if args.disciplinas:
    num_disc = args.disciplinas
else:
    num_disc = int(input("Insira a quantidade de disciplinas:"))

input_end_time = time.perf_counter()

input_time = input_end_time - input_start_time

alunos = np.arange(num_alunos)

rdd = sc.parallelize(alunos, PARTICOES)
    
current_time = time.perf_counter()
time_passed = current_time - start_time - input_time
if (time_passed >= max_tempo):
    tempo_excedido = 1
else:
    list_cr = rdd.map(calc_cr).collect()

end_time = time.perf_counter()
total_time = end_time - start_time
time_passed = total_time - input_time
if (time_passed >= max_tempo):
    print(f"Tempo de processamento máximo ({max_tempo} segundos) excedido")
else:
    print_start_time = time.perf_counter()
    num_turmas = int(np.ceil(num_alunos / 20))
    for i in range(num_turmas):
        print("----------------------------------")
        print(f"Turma {i+1}:")
        for j in range(i*20, (i+1)*20):
            if(j < len(list_cr)):
                print(f"Aluno {list_cr[j][0]} - CR: {list_cr[j][1]}")
            else:
                break
        
            
    print("----------------------------------")
    print_end_time = time.perf_counter()
    print_time = print_end_time - print_start_time
    print(f"Tempo de processamento: {time_passed} segundos")
    print(f"OBS: + {print_time} segundos (impressão)")

spark.stop()