--CREATE USER scraping WITH PASSWORD '123';
--GRANT CONNECT ON DATABASE postgres TO scraping;
--GRANT CREATE ON DATABASE postgres TO scraping;
--GRANT CREATE ON DATABASE postgres TO scraping;
--ALTER USER scraping WITH SUPERUSER;


CREATE TABLE IF NOT EXISTS prod (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        )

--select * from prod

CREATE TABLE IF NOT EXISTS producao (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        )


--select * from public.producao

--select * from public.proc_americanas_e_hibridas;
--select * from public.proc_viniferas;
--select * from public.proc_uvas_de_mesa;
--select * from public.proc_sem_classificacao;


----------------------------------------------------------------------------------------
-------------------------- CRIANDO TABELAS BRONZE ------------------------------------

CREATE TABLE IF NOT EXISTS bronze.producao (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        );
		
CREATE TABLE IF NOT EXISTS bronze.proc_americanas_e_hibridas (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        );
		
CREATE TABLE IF NOT EXISTS bronze.proc_viniferas (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        );

CREATE TABLE IF NOT EXISTS bronze.proc_uvas_de_mesa (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        );

CREATE TABLE IF NOT EXISTS bronze.proc_sem_classificacao (
            id SERIAL PRIMARY KEY,
            ano INT,
            chave TEXT,
            valor TEXT
        );



--select * from bronze.producao;

--select * from bronze.proc_americanas_e_hibridas;
--select * from bronze.proc_viniferas;
--select * from bronze.proc_uvas_de_mesa;
--select * from bronze.proc_sem_classificacao;


----------------------------------------------------------------------------------------
-------------------------- POPULANDO TABELAS BRONZE ------------------------------------


insert into bronze.producao
select * from public.producao;

insert into bronze.proc_americanas_e_hibridas
select * from public.proc_americanas_e_hibridas;

insert into bronze.proc_viniferas
select * from public.proc_viniferas;

insert into bronze.proc_uvas_de_mesa
select * from public.proc_uvas_de_mesa;

insert into bronze.proc_sem_classificacao
select * from public.proc_sem_classificacao;


----------------------------------------------------------------------------------------
-------------------------- POPULANDO TABELAS SILVER ------------------------------------

-- Criando Schema Silver
create schema silver

------ Tabela silver.processamento
--drop table silver.processamento
CREATE TABLE IF NOT EXISTS silver.processamento (
    id SERIAL PRIMARY KEY,
    ano INT,
    chave TEXT,
    valor decimal(18,2),
    origem TEXT  -- Coluna adicional para indicar a origem dos dados
);


-- Inserir dados das tabelas de processamento na tabela silver.processamento
INSERT INTO silver.processamento (ano, chave, valor, origem)
SELECT 
ano
,chave
,CAST(REPLACE(REPLACE(REPLACE(REPLACE(valor, '.', ''), '-', '0'),'nd','0'),'*','0') AS DECIMAL(12, 2))
,'Americanas e Híbridas' AS origem 
FROM bronze.proc_americanas_e_hibridas

UNION ALL

SELECT ano
,chave
,CAST(REPLACE(REPLACE(REPLACE(REPLACE(valor, '.', ''), '-', '0'),'nd','0'),'*','0') AS DECIMAL(12, 2))
,'Viníferas' AS origem 
FROM bronze.proc_viniferas

UNION ALL

SELECT 
ano
,chave
,CAST(REPLACE(REPLACE(REPLACE(REPLACE(valor, '.', ''), '-', '0'),'nd','0'),'*','0') AS DECIMAL(12, 2))
,'Uvas de mesa' AS origem 
FROM bronze.proc_uvas_de_mesa

UNION ALL

SELECT 
ano
,chave
,CAST(REPLACE(REPLACE(REPLACE(REPLACE(valor, '.', ''), '-', '0'),'nd','0'),'*','0') AS DECIMAL(12, 2))
,'Sem Classificação' AS origem 
FROM bronze.proc_sem_classificacao;

-- truncate table silver.processamento;
select * from silver.processamento;

-------------------------------------

------- Criando tabela de Produção
--drop table silver.producao
CREATE TABLE IF NOT EXISTS silver.producao (
    id SERIAL PRIMARY KEY,
    ano INT,
    chave TEXT,
    valor decimal(18,2),
    origem TEXT  -- Coluna adicional para indicar a origem dos dados
);

-- Inserindo dados dentro da tabela de Produção
INSERT INTO silver.producao (ano, chave, valor, origem)
select 
ano
,chave
,CAST(REPLACE(REPLACE(REPLACE(REPLACE(valor, '.', ''), '-', '0'),'nd','0'),'*','0') AS DECIMAL(12, 2))
,'Produção' from bronze.producao;

-- Buscando dados dentro da tabela
select * from silver.producao;

----------------------------------------------------------------------------------------
---------------- POPULANDO TABELAS DIMENSÃO NA CAMADA SILVER ---------------------------
--drop table silver.dim_ano
CREATE TABLE silver.dim_ano
(
	id_dim_ano 	serial primary key,
	ano			int
);

insert into silver.dim_ano (ano)
select distinct ano from bronze.producao order by ano


----- DIMENSÃO DE TIPO PROCESSAMENTO
--drop table silver.dim_origem
Create table silver.dim_origem
(
	ID_origem serial primary key
	,origem text
);

insert into silver.dim_origem (origem)
select distinct origem from silver.processamento order by origem;


----- DIMENSÃO DE TIPO PROCESSAMENTO -- Essa dimensão não resolve nada
--drop table silver.dim_chave_producao
Create table silver.dim_chave_processamento
(
	ID_Chave serial primary key
	,Chave varchar(50)
	,Titulo int
);

insert into silver.dim_chave_processamento (chave,titulo)
select distinct
chave
,CASE
	WHEN chave = 'TINTAS' THEN 1
	WHEN chave = 'BRANCAS' THEN 1
	WHEN chave = 'BRANCAS E ROSADAS' THEN 1
	ELSE 0
END
from silver.processamento;


----- DIMENSÃO DE TIPO PROCESSAMENTO -- Essa dimensão não resolve nada
--drop table silver.dim_chave_producao
Create table silver.dim_chave_producao
(
	ID_Chave serial primary key
	,Chave varchar(50)
	,Titulo int
);

insert into silver.dim_chave_producao (chave,titulo)
select distinct
chave
,CASE
	WHEN chave = 'VINHO DE MESA' THEN 1
	WHEN chave = 'VINHO FINO DE MESA (VINIFERA)' THEN 1
	WHEN chave = 'SUCO' THEN 1
	WHEN chave = 'DERIVADOS' THEN 1
	ELSE 0
END
from silver.processamento;





-------------------------------------working---------------------------------------------------
------------------------------ CRIANDO TABELA FATO -------------------------------------

CREATE TABLE silver.Fato_DADOS
(
	ID_FATO serial primary key
	,ID_PRODUCAO
	,ANO_ID INT
	,ID_TipoProcessamento
	,
)

select * from silver.producao



----------------------------------------------------------------------------------------
----------------------------- LISTANDO DADOS TRATADOS ----------------------------------

SELECT * FROM SILVER.DIM_ANO;
SELECT * FROM SILVER.PROCESSAMENTO;
SELECT * FROM SILVER.PRODUCAO;
SELECT * FROM SILVER.DIM_CHAVE_PROCESSAMENTO;
SELECT * FROM SILVER.DIM_CHAVE_PRODUCAO;


