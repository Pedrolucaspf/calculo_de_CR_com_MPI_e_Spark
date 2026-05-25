TEMPO_LIMITE = 60.0         # timeout em segundos — acima disso é inválido
SPEEDUP_INVALIDO = -1.0    # marcador para execuções que estouraram o tempo
TOTAL_SCRIPTS = 5          # número de scripts no log (V0, V1, V2, V3, Spark)
EXECUCOES_POR_SCRIPT = 8   # 1, 2, 4, 8, 16, 32, 64, 128 processos
SEPARADOR = "------------------------------"

def main():
    print("Lendo o arquivo mpi_experiment.log.")
    print(SEPARADOR)
    print("")

    try:
        script = "SCRIPT"
        thread = "Execução com"
        process = "Tempo de processamento"

        with open('mpi_experiment.log', 'r') as file:
            for _ in range(TOTAL_SCRIPTS):
                list_threads = []
                list_tempos = []
                proc_count = 0
                for line in file:
                    if proc_count == EXECUCOES_POR_SCRIPT:
                        break

                    if script in line:
                        print(line.strip())
                        print("")
                    elif thread in line:
                        nums = [n for n in line if n.isdigit()]
                        num_thread = int("".join(map(str, nums)))
                        list_threads.append(num_thread)
                    elif process in line:
                        num_started = 0
                        t_nums = []
                        for c in line:
                            if(num_started == 1 and c.isspace()):
                                break
                            else:
                                if (c == "-" or c.isdigit()):
                                    num_started = 1
                                    t_nums.append(c)
                                elif (c == "."):
                                    t_nums.append(c)
                            
                        tempo = float("".join(map(str, t_nums)))
                        list_tempos.append(round((tempo), 1))
                        proc_count += 1
   
                list_speedups = []
                list_eficiencias = []
                for i in range(len(list_tempos)):
                    if list_tempos[i] >= TEMPO_LIMITE:
                        list_speedups.append(SPEEDUP_INVALIDO)
                        list_eficiencias.append(SPEEDUP_INVALIDO)
                    else:
                        list_speedups.append(round((list_tempos[0] / list_tempos[i]), 1))
                        list_eficiencias.append(round((list_speedups[i] / list_threads[i]), 1))

                print(f"|   Threads  |", end="")
                for num in list_threads:
                    tamanho = 1
                    if(num > 100):
                        tamanho = 3
                    elif(num > 10):
                        tamanho = 2
                        
                    match tamanho:
                        case 3:
                            print(f"|  {num}  |", end="")
                        case 2:
                            print(f"|  {num}   |", end="")
                        case 1:
                            print(f"|   {num}   |", end="")
                print("")

                print(f"|   Speedup  |", end ="")
                for num in list_speedups:
                    if num <= (SPEEDUP_INVALIDO + 0.1):
                        print(f"|   -   |", end="")
                    else:
                        print(f"|  {num}  |", end="")
                print("")
                print(f"| Eficiência |", end="")
                for num in list_eficiencias:
                    if num <= (SPEEDUP_INVALIDO + 0.1):
                        print(f"|   -   |", end="")
                    else:
                        print(f"|  {num}  |", end="")
                print("")
                print(SEPARADOR)

    except FileNotFoundError:
        print(
            "Arquivo mpi_experiment.log não encontrado. Execute o script_cluster para gerar os dados primeiro."
        )
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")


if __name__ == "__main__":
    main()
