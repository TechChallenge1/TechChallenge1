from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import psycopg2

def scrape_producao (base_url, start_year=1970, end_year=2023):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  
    service = Service('C:/Users/.../Downloads/chromedriver-win64/chromedriver/chromedriver.exe') 

    driver = webdriver.Chrome(service=service, options=chrome_options)

    all_data = []

    for year in range(start_year, end_year + 1):
        url = f"{base_url}&ano={year}"
        driver.get(url)
        time.sleep(2)

        try:
            table = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr/td[2]/div/div/table[1]')
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                columns = row.find_elements(By.TAG_NAME, 'td')
                if len(columns) >= 2:
                    chave = columns[0].text
                    valor = columns[1].text
                    all_data.append((year, chave, valor))
        except Exception as e:
            all_data.append((year, "Erro ao raspar a tabela:", str(e)))

    driver.quit()
    return all_data

def save_producao(data, db_name='postgres', db_user='scraping', db_password='123', db_host='localhost', db_port='5432'):
    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = connection.cursor()
        
        # Cria a tabela se não existir
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS prod (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        )
        """)
        
        # Insere os dados na tabela
        for year, chave, valor in data:
            cursor.execute("INSERT INTO producao (ano, chave, valor) VALUES (%s, %s, %s)", (year, chave, valor))
        
        connection.commit()
        cursor.close()
        connection.close()
        print("Dados inseridos no banco de dados PostgreSQL com sucesso.")
    except Exception as error:
        print("Erro ao conectar ou inserir no banco de dados PostgreSQL:", error)
