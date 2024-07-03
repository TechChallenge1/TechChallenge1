import psycopg2

# Configurações de conexão
db_name = 'postgres'
db_user = 'scraping'
db_password = '123'
db_host = 'localhost'
db_port = '5432'

# Conectar ao banco de dados
try:
    connection = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    print("Conexão com o banco de dados PostgreSQL realizada com sucesso.")

    # Criar um cursor para realizar operações no banco de dados
    cursor = connection.cursor()

    # Exemplo de execução de uma consulta SQL
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print("Versão do PostgreSQL:", db_version)

    # Fechar o cursor e a conexão
    cursor.close()
    connection.close()
    print("Conexão com o banco de dados PostgreSQL fechada com sucesso.")

except Exception as error:
    print("Erro ao conectar ao banco de dados PostgreSQL:", error)
