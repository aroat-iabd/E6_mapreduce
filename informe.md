# E6 MapReduce: palabras e vendas do festival
## C1. Os libros en HDFS
### 1. Descarga en texto plano UTF-8 estes tres libros do Proxecto Gutenberg (no teu equipo ou directamente na consola do clúster):
```bash
wget https://www.gutenberg.org/cache/epub/2000/pg2000.txt
wget https://www.gutenberg.org/cache/epub/2701/pg2701.txt
wget https://www.gutenberg.org/cache/epub/1342/pg1342.txt
```
> Se a descarga falla, busca un espello (**mirror**) do Proxecto Gutenberg; por exemplo, aleph.gutenberg.org (https://aleph.gutenberg.org/cache/epub/2000/pg2000.txt). Se wget se queixa do certificado, engade *--no-check-certificate*.

### 2. Súbeos a HDFS:
```bash
hdfs dfs -mkdir -p e6/libros
hdfs dfs -put pg*.txt e6/libros/
hdfs dfs -ls e6/libros
```
- En todo o exercicio usamos estas variables na consola do clúster:
    ```bash
    J=/opt/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/jars
    EX=$(ls $J/hadoop-mapreduce-examples-*.jar)
    ST=$(ls $J/hadoop-streaming-*.jar)
    ```

#### Pregunta: o tamaño de cada libro. Con bloques de 128 MB, cantos bloques ten cada un? Cantas tarefas map esperas no seguinte apartado?


## C2. O wordcount de Hadoop
```bash
yarn jar $EX wordcount e6/libros e6/palabras
hdfs dfs -ls e6/palabras
hdfs dfs -cat e6/palabras/part-* | sort -t$'\t' -k2 -nr | head -20
```

### Preguntas:
#### 1. Os contadores ``Launched map tasks``, ``Launched reduce tasks``, ``Map input records``, ``Map output records`` e ``Reduce output records``. Que conta cada un neste traballo? Comproba ``Map input records`` e ``Map output records`` con ``wc``: cadran? Cadra o número de map co que esperabas?

#### 2. As 20 palabras máis frecuentes. Ves algún problema? Busca na saída whale (``grep -i "^whale"``): cantas variantes aparecen e por que?

#### 3. Que ficheiros hai en ``e6/palabras`` e para que serve cada un? Cantos ``part-`` hai? Por que ese número, se no comando non o indicaches? (Pista: compara co contador ``Launched reduce tasks``.)

#### 4. Que pasa se executas o mesmo comando outra vez? Por que?


## C3. Compras por barra: o exemplo
### Pasos:
- O xerador ``xera_compras.py`` crea as compras de tres noites do festival, unha por liña
```text
compra_id,data_hora,barra,pulseira,produto,unidades,importe
41,2027-07-16 18:00:38,Praia,P-4439,viño,3,10.50
```
- Hai catro barras (Praia, Norte, Escenario e VIP) e o importe vai en euros, con dous decimais
```bash
python xera_compras.py --saida compras.csv
hdfs dfs -mkdir -p e6/compras
hdfs dfs -put compras.csv e6/compras/
```
- Dous scripts que **contan as compras de cada barra**:
    - ``mapper_compras.py``: por cada compra, emite ``barra<TAB>1``.
    - ``reducer_suma.py``: suma os valores de cada clave. Como os recibe ordenados, abonda con lembrar a clave anterior para saber cando cambia.

#### 1. Probar os dous scripts en local cun anaco do ficheiro.
```bash
head -1000 compras.csv | python mapper_compras.py
head -1000 compras.csv | python mapper_compras.py | sort | python reducer_suma.py
```

#### 2. Lanzar o clúster con Hadoop Streaming
```bash
yarn jar $ST -files mapper_compras.py,reducer_suma.py \
 -mapper "python mapper_compras.py" -reducer "python reducer_suma.py" \
 -input e6/compras -output e6/barras
hdfs dfs -cat e6/barras/part-*
```

### Preguntas:
#### 1.  O resultado: compras de cada barra. Suman o total de compras do ficheiro? Compara ``Map input records`` e ``Map output records`` co número de liñas de compras.csv (``wc -l``): por que non coinciden os tres?

#### 2. Que fai a liña ``if len(campos) != 7 or campos[0] == "compra_id"`` do mapper? Que pasaría sen ela?

#### 3. Por que o reducer pode supoñer que todas as liñas dunha barra lle chegan seguidas? Quen as ordena no clúster?

#### 4. Cantos ficheiros ``part-`` hai en e6/barras e cantos teñen datos? Por que? Podes ver os tamaños con ``hdfs dfs -ls e6/barras`` e contar os que non están baleiros con ``hdfs dfs -ls e6/barras/part-* | awk '$5 > 0' | wc -l``.


## C4. Modificar o mapper: importe por barra
### Pasos:
#### 1. Copia ``mapper_compras.py`` como ``mapper_importes.py`` e modifícao para que, co mesmo ``reducer_suma.py``, o resultado sexa o importe total de cada barra. Non cambies o reducer.

#### 2. Proba en local e despois no clúster, coa saída en e6/importes.
- Local:

- Clúster:

### Preguntas:
#### 1. O teu mapper e que cambiaches.

#### 2. O importe total de cada barra.

#### 3. Por que vale o mesmo reducer para contar compras e para sumar importes?

#### 4. O reducer usa ``Decimal`` en lugar de ``float``. Proba a sumar en Python ``0.1 + 0.2``: por que importa isto cando se suman cartos?


## Ao rematar:
```bash
hdfs dfs -rm -r -skipTrash e6
```
