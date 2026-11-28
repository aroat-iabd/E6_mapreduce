# E6 MapReduce: palabras e vendas do festival
## C1. Os libros en HDFS
### 1. Descarga en texto plano UTF-8 estes tres libros do Proxecto Gutenberg (no teu equipo ou directamente na consola do clúster):
```bash
wget https://aleph.gutenberg.org/cache/epub/2000/pg2000.txt --no-check-certificate
wget https://aleph.gutenberg.org/cache/epub/2701/pg2701.txt --no-check-certificate
wget https://aleph.gutenberg.org/cache/epub/1342/pg1342.txt --no-check-certificate
```
> Se a descarga falla, busca un espello (**mirror**) do Proxecto Gutenberg; por exemplo, aleph.gutenberg.org (https://aleph.gutenberg.org/cache/epub/2000/pg2000.txt). Se wget se queixa do certificado, engade *--no-check-certificate*.

### 2. Súbeos a HDFS:
```bash
hdfs dfs -mkdir -p e6/libros
hdfs dfs -put pg*.txt e6/libros/
hdfs dfs -ls e6/libros
```
```text
Found 3 items
-rw-r--r--   3 xuedua077 xunta     772386 2026-11-27 19:52 e6/libros/pg1342.txt
-rw-r--r--   3 xuedua077 xunta    2225777 2026-11-27 19:52 e6/libros/pg2000.txt
-rw-r--r--   3 xuedua077 xunta    1276315 2026-11-27 19:52 e6/libros/pg2701.txt
```

- En todo o exercicio usamos estas variables na consola do clúster:
    ```bash
    J=/opt/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/jars
    EX=$(ls $J/hadoop-mapreduce-examples-*.jar)
    ST=$(ls $J/hadoop-streaming-*.jar)
    ```

#### Pregunta
- O tamaño de cada libro.
A partir da saída do comando `hdfs dfs -ls e6/libros`, o tamaño de cada ficheiro é o seguinte:

| Ficheiro | Tamaño en bytes | Tamaño aproximado |
| :--- | :--- | :--- |
| `pg1342.txt` | 772.386 bytes | ~754 KB (~0,74 MB) |
| `pg2000.txt` | 2.225.777 bytes | ~2,12 MB |
| `pg2701.txt` | 1.276.315 bytes | ~1,22 MB |

- Con bloques de 128 MB, cantos bloques ten cada un?
**1 bloque por cada libro (3 bloques en total).**
**Xustificación:** Un bloque de 128 MB equivale a $134.217.728$ bytes ($128 \times 1024 \times 1024$). Dado que ningún dos tres ficheiros supera ese tamaño e que en HDFS os ficheiros distintos non comparten bloques entre si, cada libro almacénase nun único bloque.

- Cantas tarefas map esperas no seguinte apartado?
**Espéranse 3 tarefas *map*** (1 por cada bloque/ficheiro).
**Xustificación:** Por defecto, MapReduce (usando `TextInputFormat`) xera un *InputSplit* por cada bloque de HDFS. Ao haber 3 bloques distintos no directorio de entrada, lanzaranse 3 tarefas *map* en paralelo (unha por libro).


## C2. O wordcount de Hadoop
```bash
yarn jar $EX wordcount e6/libros e6/palabras
hdfs dfs -ls e6/palabras
hdfs dfs -cat e6/palabras/part-* | sort -t$'\t' -k2 -nr | head -20
```
```text
26/11/27 20:13:25 INFO input.FileInputFormat: Total input files to process : 3
26/11/27 20:13:25 INFO mapreduce.JobSubmitter: number of splits:3
...
26/11/27 20:13:32 INFO mapreduce.Job:  map 0% reduce 0%
26/11/27 20:13:37 INFO mapreduce.Job:  map 100% reduce 0%
26/11/27 20:13:45 INFO mapreduce.Job:  map 100% reduce 100%
26/11/27 20:13:45 INFO mapreduce.Job: Job job_1764063724163_36893 completed successfully
26/11/27 20:13:45 INFO mapreduce.Job: Counters: 54
        File System Counters
                HDFS: Number of bytes read=4274838
                HDFS: Number of bytes written=903338
        Job Counters
                Launched map tasks=3
                Launched reduce tasks=48
                Rack-local map tasks=3
                Total time spent by all map tasks (ms)=8975
                Total time spent by all reduce tasks (ms)=221069
        Map-Reduce Framework
                Map input records=75303
                Map output records=735917
                Map output bytes=7121806
                Combine input records=735917
                Combine output records=88024
                Reduce input groups=79998
                Reduce shuffle bytes=918000
                Reduce input records=88024
                Reduce output records=79998
                Shuffled Maps =144
        File Input Format Counters
                Bytes Read=4274478
        File Output Format Counters
                Bytes Written=903338
```
```text
Found 49 items
-rw-r--r--   3 xuedua077 xunta          0 2026-11-27 20:13 e6/palabras/_SUCCESS
-rw-r--r--   3 xuedua077 xunta      19039 2026-11-27 20:13 e6/palabras/part-r-00000
-rw-r--r--   3 xuedua077 xunta      18747 2026-11-27 20:13 e6/palabras/part-r-00001
-rw-r--r--   3 xuedua077 xunta      17402 2026-11-27 20:13 e6/palabras/part-r-00002
...
-rw-r--r--   3 xuedua077 xunta      18824 2026-11-27 20:13 e6/palabras/part-r-00045
-rw-r--r--   3 xuedua077 xunta      19250 2026-11-27 20:13 e6/palabras/part-r-00046
-rw-r--r--   3 xuedua077 xunta      18221 2026-11-27 20:13 e6/palabras/part-r-00047
```
```text
que     19546
the     18539
de      18195
a       16197
y       15976
of      10652
la      10331
and     9508
to      8878
el      8009
en      7941
no      6513
in      5891
se      4751
los     4701
that    4161
con     4119
por     3763
his     3624
I       3487
```

### Preguntas:
#### 1. Os contadores ``Launched map tasks``, ``Launched reduce tasks``, ``Map input records``, ``Map output records`` e ``Reduce output records``. Que conta cada un neste traballo? Comproba ``Map input records`` e ``Map output records`` con ``wc``: cadran? Cadra o número de map co que esperabas?

- **Significado de cada contador neste traballo:**
  - **`Launched map tasks` (`3`):** Número total de tarefas *map* lanzadas por YARN para procesar os *splits* de entrada (1 por cada libro).
  - **`Launched reduce tasks` (`48`):** Número total de tarefas *reduce* executadas para agrupar e sumar as frecuencias das palabras.
  - **`Map input records` (`75.303`):** Número de rexistros lidos polos *mappers*. Ao usar `TextInputFormat`, cada rexistro equivale a **unha liña de texto** dos ficheiros de entrada.
  - **`Map output records` (`735.917`):** Número total de pares `(palabra, 1)` emitidos polos *mappers* antes de pasar polo *Combiner*, é dicir, o **total de palabras (tokens)** extraídas dos tres libros.
  - **`Reduce output records` (`79.998`):** Número total de pares `(palabra, frecuencia_total)` escritos en HDFS polos *reducers*, que representa o **vocabulario único** (palabras/tokens distintos).

- **Comprobación con `wc`:**
  Ao executar `wc -l -w pg*.txt` sobre os ficheiros locais:
  ```text
  14915  130412 pg1342.txt
  38057  386668 pg2000.txt
  22331  215840 pg2701.txt
  75303  732920 total
  ```
  - **Cadran?** As **liñas (`75.303`) cadran exactamente** con `Map input records` (`75.303`). Nas **palabras** hai unha pequena diferenza de 2.997 tokens (`732.920` en `wc -w` fronte a `735.917` en `Map output records`), xa que `wc -w` usa o *locale* UTF-8 do sistema mentres que o `WordCount` de Java emprega `StringTokenizer` (que separa estritamente polos 5 caracteres ASCII en branco: `" \t\n\r\f"`).

- **Cadra o número de *map* co esperado?**
  **Si.** Na pregunta C1 esperábanse **3 tarefas *map*** (1 por cada ficheiro/bloque de HDFS ao pesaren todos menos de 128 MB) e o contador confirma `Launched map tasks = 3`.

#### 2. As 20 palabras máis frecuentes. Ves algún problema? Busca na saída whale (``grep -i "^whale"``): cantas variantes aparecen e por que?

- **Problemas detectados no Top 20:**
  1. **Predominio de *stopwords* (palabras baleiras):** Os primeiros postos están ocupados por artigos, preposicións, conxuncións e pronomes (*que, the, de, a, y, of, la, and...*) que non achegan información sobre o contido temático dos libros.
  2. **Falta de normalización (maiúsculas/minúsculas e puntuación):** O `WordCount` estándar non converte o texto a minúsculas nin elimina signos de puntuación (puntos, comas, comiñas, apóstrofes, guións), o que fragmenta o reconto dunha mesma palabra en múltiples claves distintas.
  3. **Mestura de idiomas:** Ao procesar conxuntamente *Don Quijote* (español) con *Moby Dick* e *Pride and Prejudice* (inglés), mestúranse vocabularios de dúas linguas distintas no mesmo ranking.

- **Busca de `whale` (`hdfs dfs -cat e6/palabras/part-* | grep -i "^whale" | wc -l`):**
  - Aparecen **173 variantes** distintas (como `whale`, `Whale`, `WHALE`, `whales`, `whale's`, `whale.`, `whale,`, `whale;`, `whale-ship`, `whaleboat`, etc.).
  - **Por que?** Porque o `WordCount` por defecto distingue entre maiúsculas e minúsculas (*case-sensitive*) e só parte o texto polos espazos en branco, deixando pegados á palabra os signos de puntuación adxacentes, xenitivos saxóns (`'s`) ou palabras compostas con guión.

#### 3. Que ficheiros hai en ``e6/palabras`` e para que serve cada un? Cantos ``part-`` hai? Por que ese número, se no comando non o indicaches? (Pista: compara co contador ``Launched reduce tasks``.)

- **Contido do directorio `e6/palabras` (49 elementos en total):**
  - **`_SUCCESS` (1 ficheiro de 0 bytes):** Ficheiro marcador (bandeira) baleiro que crea Hadoop ao rematar o traballo para indicar que todos os *reducers* finalizaron sen erros e os datos son consistentes.
  - **`part-r-00000` a `part-r-00047` (48 ficheiros):** Ficheiros de saída que conteñen os resultados reais do traballo (pares `palabra\tfrecuencia` ordenados alfabeticamente dentro de cada partición).

- **Por que hai 48 ficheiros `part-`?**
  Porque cada tarefa *reduce* escribe a súa saída nun ficheiro `part-r-XXXXX` independente, e neste traballo lanzáronse **48 *reducers*** (`Launched reduce tasks = 48`). Ao non indicar explicitamente o número de *reducers* na liña de comandos, Hadoop aplicou o valor configurado por defecto no clúster (`mapreduce.job.reduces = 48`).

#### 4. Que pasa se executas o mesmo comando outra vez? Por que?

- **Resultado:** O traballo **falla inmediatamente** antes de iniciar ningunha tarefa *map* ou *reduce*, lanzando a excepción:
  `org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://.../user/xuedua077/e6/palabras already exists`
- **Por que?** Como medida de seguridade para evitar a perda accidental de datos de execucións anteriores, `FileOutputFormat` de Hadoop MapReduce esixe que o directorio de saída **non exista previamente** en HDFS. Para volver executalo, habería que borrar primeiro o directorio (`hdfs dfs -rm -r e6/palabras`) ou especificar unha ruta de saída diferente.


## C3. Compras por barra: o exemplo
### Pasos:
- O xerador ``xera_compras.py`` crea as compras de tres noites do festival, unha por liña
```text
compra_id,data_hora,barra,pulseira,produto,unidades,importe
41,2027-07-16 18:00:38,Praia,P-4439,viño,3,10.50
```
- Enviar os scripts ao clúster
```bash
scp xera_compras.py mapper_compras.py reducer_suma.py xuedua077@cdh61-login2.bd.cluster.cesga.es:~/
```
- Comprobar que se cargaron ben no clúster
```bash
ls -l *.py
```
```text
-rw-r--r-- 1 xuedua077 xunta  530 Nov 28 13:30 mapper_compras.py
-rw-r--r-- 1 xuedua077 xunta  851 Nov 28 13:30 reducer_suma.py
-rw-r--r-- 1 xuedua077 xunta 2547 Nov 28 13:30 xera_compras.py
-rw-r--r-- 1 xuedua077 xunta 2097 Nov 18 21:08 xera_historico.py
```
- Hai catro barras (Praia, Norte, Escenario e VIP) e o importe vai en euros, con dous decimais
```bash
python xera_compras.py --saida compras.csv
hdfs dfs -mkdir -p e6/compras
hdfs dfs -put compras.csv e6/compras/
```
```text
compras.csv: 90000 compras
```
- Dous scripts que **contan as compras de cada barra**:
    - ``mapper_compras.py``: por cada compra, emite ``barra<TAB>1``.
    - ``reducer_suma.py``: suma os valores de cada clave. Como os recibe ordenados, abonda con lembrar a clave anterior para saber cando cambia.

#### 1. Probar os dous scripts en local cun anaco do ficheiro.
```bash
head -1000 compras.csv | python mapper_compras.py
head -1000 compras.csv | python mapper_compras.py | sort | python reducer_suma.py
```
```text
Escenario       293
Norte   253
Praia   348
VIP     105
```

#### 2. Lanzar o clúster con Hadoop Streaming
```bash
yarn jar $ST -files mapper_compras.py,reducer_suma.py \
 -mapper "python mapper_compras.py" -reducer "python reducer_suma.py" \
 -input e6/compras -output e6/barras
hdfs dfs -cat e6/barras/part-*
```
```text
VIP     9108
Norte   22646
Escenario       26896
Praia   31350
```

### Preguntas:
#### 1. O resultado: compras de cada barra. Suman o total de compras do ficheiro? Compara ``Map input records`` e ``Map output records`` co número de liñas de compras.csv (``wc -l``): por que non coinciden os tres?

* **Resultado obtido por barra:**
  * **Praia:** `31.350`
  * **Escenario:** `26.896`
  * **Norte:** `22.646`
  * **VIP:** `9.108`
* **Suman o total de compras do ficheiro?**
  **Si:** $31.350 + 26.896 + 22.646 + 9.108 = \mathbf{90.000}$ compras, que coincide exactamente co total xerado en `compras.csv`.

* **Comparación entre `wc -l`, `Map input records` e `Map output records`:**
  * `wc -l compras.csv` = **90.001** liñas.
  * `Map input records` = **90.001** rexistros.
  * `Map output records` = **90.000** rexistros.
  * **Por que non coinciden os tres?** Porque `compras.csv` contén **90.000 liñas de compras** máis **1 liña de cabeceira** (`compra_id,data_hora,barra,...`), facendo un total de 90.001 liñas. Hadoop le todas as liñas de entrada (`Map input records = 90.001`, igual ca `wc -l`), pero `mapper_compras.py` descarta a cabeceira sen emitir nada para ela, polo que a saída dos *mappers* é de exactamente `90.000` pares clave-valor.

#### 2. Que fai a liña ``if len(campos) != 7 or campos[0] == "compra_id"`` do mapper? Que pasaría sen ela?

* **Que fai?** Filtra e ignora a liña de cabeceira do CSV (`campos[0] == "compra_id"`) e calquera liña baleira ou malformada que non teña exactamente as 7 columnas esperadas (`len(campos) != 7`).
* **Que pasaría sen ela?**
  1. Procesaría a cabeceira coma se fose unha venda real e emitiría `barra	1`, creando na saída final unha quinta barra falsa chamada **`barra`** cun reconto de `1`.
  2. Se houbese unha liña baleira ao final do ficheiro, ao tentar acceder a `campos[2]` o script lanzaría un `IndexError` e o traballo fallaría.

#### 3. Por que o reducer pode supoñer que todas as liñas dunha barra lle chegan seguidas? Quen as ordena no clúster?

* **Por que chegan seguidas?** Porque o paradigma MapReduce garante que todos os valores asociados a unha mesma clave se agrupan e se entregan contiguos ao *reducer*. Deste xeito, o *reducer* non necesita gardar todas as claves na memoria RAM á vez e abóndalle con ir acumulando a suma mentres a clave actual coincida coa anterior.
* **Quen as ordena no clúster?** O propio ***framework* de Hadoop MapReduce** durante a fase intermedia de **Shuffle & Sort** (entre a fase *Map* e a fase *Reduce*), realizando de xeito distribuído o equivalente ao comando `sort` da proba en local.

#### 4. Cantos ficheiros ``part-`` hai en e6/barras e cantos teñen datos? Por que? Podes ver os tamaños con ``hdfs dfs -ls e6/barras`` e contar os que non están baleiros con ``hdfs dfs -ls e6/barras/part-* | awk '$5 > 0' | wc -l``.

* **Cantos ficheiros hai e cantos teñen datos?**
  * Hai **48 ficheiros `part-`** en total (`part-00000` a `part-00047`), correspondentes aos 48 *reducers* lanzados por defecto no clúster (`Launched reduce tasks = 48`).
  * Porén, **como máximo 4 ficheiros `part-` teñen datos** (`tamaño > 0`, sumando 49 bytes en total) e os outros 44 están completamente baleiros (`0` bytes).
* **Por que?** Porque neste traballo só existen **4 claves distintas** (`Escenario`, `Norte`, `Praia` e `VIP`). Na fase de particionado (*Partitioner*), Hadoop aplica unha función *hash* a cada clave para asignala a un único *reducer*: como só hai 4 claves, como moito 4 *reducers* reciben datos e escriben no seu ficheiro de saída, mentres que os outros 44 *reducers* non reciben ningún rexistro e xeran ficheiros baleiros.

## C4. Modificar o mapper: importe por barra
### Pasos:
#### 1. Copia ``mapper_compras.py`` como ``mapper_importes.py`` e modifícao para que, co mesmo ``reducer_suma.py``, o resultado sexa o importe total de cada barra. Non cambies o reducer.
```bash
nano mapper_importes.py
```

#### 2. Proba en local e despois no clúster, coa saída en e6/importes.
- Local: `head -1000 compras.csv | python mapper_importes.py | sort | python reducer_suma.py`
```text
Escenario       2095.00
Norte   1803.00
Praia   2211.00
VIP     815.50
```

- Clúster:
```bash
yarn jar $ST -files mapper_importes.py,reducer_suma.py \
 -mapper "python mapper_importes.py" -reducer "python reducer_suma.py" \
 -input e6/compras -output e6/importes
```
```text
..._36918 completed successfully
26/11/28 15:04:38 INFO mapreduce.Job: Counters: 54
        File System Counters
                HDFS: Number of bytes read=4937652
                HDFS: Number of bytes written=65
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=48
        Map-Reduce Framework
                Map input records=90001
                Map output records=90000
                Map output bytes=1101981
                Reduce input groups=4
                Reduce shuffle bytes=277221
                Reduce input records=90000
                Reduce output records=4
        File Input Format Counters
                Bytes Read=4937434
        File Output Format Counters
                Bytes Written=65
26/11/28 15:04:38 INFO streaming.StreamJob: Output directory: e6/importes
```

### Preguntas:
#### 1. O teu mapper e que cambiaches.
```python
# -*- coding: utf-8 -*-
"""E6: mapper que emite (barra, importe) por cada compra."""
from __future__ import print_function
import sys

for linha in sys.stdin:
    campos = linha.strip().split(",")
    if len(campos) != 7 or campos[0] == "compra_id":
        continue
    barra = campos[2]
    importe = campos[6]
    print("%s\t%s" % (barra, importe))
```
- **Que cambiaches respecto a `mapper_compras.py`?**
  Na última liña do script, en lugar de emitir sempre o valor constante `1` por cada compra (`print("%s\t1" % barra)`), extráese a séptima columna do CSV (`campos[6]`, correspondente ao `importe` en euros) e emítese como valor asociado á barra (`print("%s\t%s" % (barra, importe))`).

#### 2. O importe total de cada barra.
```bash
hdfs dfs -cat e6/importes/part-*
```
```text
VIP     62176.00
Norte   155114.50
Escenario       184230.50
Praia   212840.50
```

#### 3. Por que vale o mesmo reducer para contar compras e para sumar importes?
Porque `reducer_suma.py` actúa como un **sumador xenérico por clave**: recibe pares `(clave, valor_numérico)` ordenados por clave e vai acumulando a suma de todos os valores asociados mentres a clave non cambie.
* En **C3**, o *mapper* enviaba uns (`1`) por cada rexistro, polo que sumar eses valores equivalía a **contar** o número de compras.
* En **C4**, o *mapper* envía o importe decimal (`campos[6]`), polo que a mesma operación de suma calcula o **importe total facturado** en cada barra sen ter que modificar o *reducer*.

#### 4. O reducer usa ``Decimal`` en lugar de ``float``. Proba a sumar en Python ``0.1 + 0.2``: por que importa isto cando se suman cartos?
Ao executar `0.1 + 0.2` en Python usando `float`, o resultado obtido é **`0.30000000000000004`** en lugar de `0.3`.

- **Por que importa ao sumar cartos?**
  Os números `float` empregan representación binaria en coma flotante (estándar IEEE 754), que non pode representar con exactitude moitas fraccións decimais sinxelas como `0.10` € ou `0.20` €. Ao sumar decenas de miles de transaccións monetarias (90.000 compras neste caso), eses pequenos erros de precisión binaria vanse acumulando e provocan desaxustes nos céntimos. O tipo `Decimal` realiza a aritmética en base 10 exacta, garantindo que os cálculos financeiros cadren ao céntimo.

## Ao rematar:
```bash
hdfs dfs -rm -r -skipTrash e6
```