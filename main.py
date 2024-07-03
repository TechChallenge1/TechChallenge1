from fastapi import FastAPI
from api.v1.enpoints.producao.api_producao import scrape_producao, save_producao
from api.v1.enpoints.processamento.api_processamento import scrape_processamento, save_processamento 

app = FastAPI(
    title="Documentação API || Curso Machine Learning Engineering || Grupo 2",
    version="0.0.1",
    description="Esta API foi desenvolvida com o objetivo de guiar o grupo na criação de um projeto para o Tech Challenge."
)

@app.get("/producao",
        description='Produção de vinhos, sucos e derivados',
        summary='Retorna todos os dados desde de 1970 a 2023',
        tags=['Produção de Vinhos']
        )
def read_producao(base_url: str = "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_02"):
    result = scrape_producao(base_url)
    save_producao(result) 
    return {"data": result}

@app.get("/processamento/{categoria}",
        description='Dados de processamento de vinhos, sucos e derivados',
        summary='Retorna todos os dados desde de 1970 a 2023',
        tags=['Processamento de Vinhos']
        )
def read_processamento(categoria: str):
    categorias = {
        "viniferas": {
            "url": "http://vitibrasil.cnpuv.embrapa.br/index.php?subopcao=subopt_01&opcao=opt_03",
            "xpath": "/html/body/table[4]/tbody/tr/td[2]/div/div/table[1]"
        },
        "americanas_e_hibridas": {
            "url": "http://vitibrasil.cnpuv.embrapa.br/index.php?subopcao=subopt_02&opcao=opt_03",
            "xpath": "/html/body/table[4]/tbody/tr/td[2]/div/div/table[1]"
        },
        "uvas_de_mesa": {
            "url": "http://vitibrasil.cnpuv.embrapa.br/index.php?subopcao=subopt_03&opcao=opt_03",
            "xpath": "/html/body/table[4]/tbody/tr/td[2]/div/div/table[1]"
        },
        "sem_classificacao": {
            "url": "http://vitibrasil.cnpuv.embrapa.br/index.php?subopcao=subopt_04&opcao=opt_03",
            "xpath": "/html/body/table[4]/tbody/tr/td[2]/div/div/table[1]"
        }
    }

    if categoria not in categorias:
        return {"error": "Categoria não encontrada"}

    url = categorias[categoria]["url"]
    xpath = categorias[categoria]["xpath"]
    result = scrape_processamento(url, xpath)
    save_processamento(result, f"proc_{categoria}")  
    return {"data": result}
