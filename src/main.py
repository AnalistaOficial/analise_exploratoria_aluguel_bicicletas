import duckdb
import os

base_dir = os.getcwd()  # Diretório atual
parent_dir = os.path.join(base_dir, "..")
data_dir = os.path.join(parent_dir, "data")  # Caminho da pasta "data"
file_path = os.path.join(data_dir, "dataset.csv")  # Caminho completo do arquivo

# Conectar ao DuckDB (em memória)
con = duckdb.connect(database=':memory:')

# Ler o arquivo CSV
con.execute(f"""
    CREATE TABLE alugueis AS
    SELECT * FROM read_csv_auto('{file_path}');
""")

print('#############################################################################################################')
print("1. Quais são as Top 5 estações com maior número de aluguel de bikes?")
print('#############################################################################################################')

resultado1 = con.execute("""
    SELECT Estacao_Aluguel, COUNT(*) AS Total_Alugueis
    FROM alugueis
    GROUP BY Estacao_Aluguel
    ORDER BY Total_Alugueis DESC
    LIMIT 5;
""").fetchdf()
print("\n", resultado1, "\n")

print('#############################################################################################################')
print("2. Quais são as Top 5 rotas, com base na estação inicial e final, e a média de duração de cada aluguel?")
print('#############################################################################################################')

resultado2 = con.execute("""
    SELECT Estacao_Aluguel, Estacao_Chegada,
           AVG(epoch(strptime(Data_Chegada || ' ' || Hora_Chegada, '%Y-%m-%d %H:%M:%S')) -
               epoch(strptime(Data_Aluguel || ' ' || Hora_Aluguel, '%Y-%m-%d %H:%M:%S'))) AS Media_Duracao_Segundos
    FROM alugueis
    GROUP BY Estacao_Aluguel, Estacao_Chegada
    ORDER BY Media_Duracao_Segundos DESC
    LIMIT 5;
""").fetchdf()
print("\n", resultado2, "\n")

print('#############################################################################################################')
print("3. Quem aluga mais bikes, homens ou mulheres? Qual o tempo médio de aluguel de bikes?\n")
print('#############################################################################################################')

resultado3 = con.execute("""
    SELECT Genero_Usuario,
           COUNT(*) AS Total_Alugueis,
           AVG(epoch(strptime(Data_Chegada || ' ' || Hora_Chegada, '%Y-%m-%d %H:%M:%S')) -
               epoch(strptime(Data_Aluguel || ' ' || Hora_Aluguel, '%Y-%m-%d %H:%M:%S'))) AS Media_Duracao_Segundos
    FROM alugueis
    GROUP BY Genero_Usuario
    ORDER BY Total_Alugueis DESC;
""").fetchdf()
print("\n", resultado3 , "\n")

print('#############################################################################################################')
print("4. Qual faixa etária aluga mais bikes? Qual o tempo médio de aluguel de bikes?\n")
print('#############################################################################################################')

resultado4 = con.execute("""
    SELECT CASE 
               WHEN Idade_Usuario BETWEEN 18 AND 25 THEN '18-25'
               WHEN Idade_Usuario BETWEEN 26 AND 35 THEN '26-35'
               WHEN Idade_Usuario BETWEEN 36 AND 45 THEN '36-45'
               WHEN Idade_Usuario BETWEEN 46 AND 60 THEN '46-60'
               ELSE '60+' 
           END AS Faixa_Etaria,
           COUNT(*) AS Total_Alugueis,
           AVG(epoch(strptime(Data_Chegada || ' ' || Hora_Chegada, '%Y-%m-%d %H:%M:%S')) -
               epoch(strptime(Data_Aluguel || ' ' || Hora_Aluguel, '%Y-%m-%d %H:%M:%S'))) AS Media_Duracao_Segundos
    FROM alugueis
    GROUP BY Faixa_Etaria
    ORDER BY Total_Alugueis DESC;
""").fetchdf()
print("\n", resultado4 , "\n")

# Quais são as estações com maior número de bikes alugadas
resultado5 = con.execute("""
    SELECT Estacao_Aluguel AS Estacao, COUNT(*) AS Total_Alugueis
    FROM alugueis
    GROUP BY Estacao_Aluguel
    ORDER BY Total_Alugueis DESC
    LIMIT 5;
""").fetchdf()

# Quais são as estações com maior número de bikes devolvidas
resultado6 = con.execute("""
    SELECT Estacao_Chegada AS Estacao, COUNT(*) AS Total_Devolucoes
    FROM alugueis
    GROUP BY Estacao_Chegada
    ORDER BY Total_Devolucoes DESC
    LIMIT 5;
""").fetchdf()

print('#############################################################################################################')
print("5. Quais são as estações com maior número de bikes alugadas?")
print('#############################################################################################################')
print("\n", resultado5, "\n")

print('#############################################################################################################')
print("\n5. Quais são as estações com maior número de bikes devolvidas?")
print('#############################################################################################################')
print("\n", resultado6)
