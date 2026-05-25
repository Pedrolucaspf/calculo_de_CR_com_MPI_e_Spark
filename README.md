# Cálculo de CR com MPI e Spark
Projeto em que cinco implementações de algoritmos com paralelismo (quatro em MPI e uma em Spark) têm suas performances comparadas entre si.

Os códigos implementam o cálculo do CR (coeficiente de rendimento) da quantidade de alunos que for inserida, os quais cursaram um número igual de disciplinas que também é inserido. A nota e carga horária de cada disciplina são gerados aleatoriamente para cada aluno.

## Pipeline de Otimizações

As versões MPI demonstram **3 alterações incrementais** com impacto no desempenho:

| Versão | Arquivo | Estratégia | Primitivas |
|---|---|---|---|
| **V0** | `calc_cr_mpi_broadcast.py` | Linha de base: `random`, loops Python, `gather` | `bcast` + `gather` |
| **V1** | `calc_cr_mpi_broadcast_numpy.py` | Alteração 1: NumPy vetorizado | `bcast` + `gather` |
| **V2** | `calc_cr_mpi_scatterv_gather.py` | Alteração 2: Geração centralizada + `Scatterv` | `bcast` + `Scatterv` + `gather` |
| **V3** | `calc_cr_mpi_scatterv.py` | Alteração 3: `Gatherv` binário | `bcast` + `Scatterv` + `Gatherv` |
| Spark | `calc_cr_spark.py` | RDD com `parallelize` + `map` | — |

### As 3 Alterações

1. **Loops Python → NumPy vetorizado** (V0→V1): Substitui `for j in range(num_disc)` por `np.sum(array_notas * array_horas)`, executando o cálculo em C compilado ao invés do interpretador Python.
2. **Geração distribuída → centralizada com Scatterv** (V1→V2): Apenas o processo raiz gera os dados, eliminando trabalho redundante. Os arrays são distribuídos via `Scatterv`, que permite tamanhos diferentes por processo.
3. **`gather` (pickle) → `Gatherv` binário** (V2→V3): Substitui a serialização pickle do Python por `MPI.DOUBLE` direto, eliminando overhead de serialização.

Um script (o arquivo script_cluster) é responsável pela execução das cinco implementações com diferentes quantidades de processos, salvando os tempos de processamento em um log.

O arquivo analise.py lê este log e calcula as métricas de speedup e eficiência para cada execução em comparação com a serial.

## Pré-requisitos
Para rodar este projeto, você precisa ter instalado:

- **Python 3.12+**
- **uv**: Pode ser instalado via curl no Linux/WSL:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- **OpenMPI** 
  ```bash
  sudo apt install openmpi-bin libopenmpi-dev -y
  ```
- **Java**: Necessário para o funcionamento do Pyspark
  ```bash
  sudo apt install default-jdk
  ```

## Instalação das dependências

Na raiz do projeto, instale as bibliotecas necessárias (Mpi4Py, Numpy e Pyspark) executando:
 
  ```bash
  uv sync
  ```

## Como executar as implementações

O ideal é executar o script_cluster, para que as comparações sejam feitas. Se desejar executá-las individualmente, busque os comandos dentro do próprio script e insira-os na linha de comando. 

### 1. Dar permissões de acesso ao script
Necessário para que ele seja executado.
  ```bash
  chmod +x script_cluster
  ```
### 2. Escolher a quantidade de alunos e disciplinas (Opcional)
Abra o script com o comando a seguir e edite o número de alunos e disciplinas que deseja nos parâmetros das execuções dos arquivos. Por padrão, foram escolhidas as quantidades 200.000 (alunos) e 60 (disciplinas). Note que isso deve ser alterado em duas linhas do script, pois o comando da implementação com Spark difere das de MPI. 
  ```bash
  nano script_cluster
  ```

### 3. Executar o script
Ele executará automaticamente cada implementação com 1, 2, 4, 8,...,128 processos, e gerará o arquivo mpi_experiment.log, que guarda os tempos de processamento.  

  ```bash
  ./script_cluster
  ```

### 4. Rodar Análise
O arquivo de análise imprime na tela uma tabela para cada uma das três implementações. Ela mostra a quantidade de threads, o speedup e a eficiência em cada execução. Ele pode ser executado com o comando a seguir:
  ```bash
  uv run python analise.py
  ```
