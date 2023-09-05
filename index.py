import requests as req
import json
import psycopg2


def encontrar_chave(dicionario, chave):
    if isinstance(dicionario, dict):  # Verifica se é um dicionário
        if chave in dicionario:  # Verifica se a chave está no dicionário atual
            return dicionario[chave]
        else:
            for valor in dicionario.values():  # Percorre os valores do dicionário
                resultado = encontrar_chave(valor, chave)
                if resultado is not None:
                    return resultado
    elif isinstance(dicionario, list):  # Verifica se é uma lista
        for item in dicionario:  # Percorre os itens da lista
            resultado = encontrar_chave(item, chave)
            if resultado is not None:
                return resultado
    return None  # Retorna None se a chave não for encontrada


flag = False  # para nao ficar requisitando da api em todo teste

# formato requerido URL : https://re.jrc.ec.europa.eu/api/v5_2/DRcalc?lat=-1.0714&lon=-48.1264&month=9&global=1&raddatabase=PVGIS-SARAH2&localtime=1&angle=10&aspect=180&showtemperatures=1&outputformat=json

api_url = 'https://re.jrc.ec.europa.eu/api/v5_2/DRcalc'

api_headers = {'Accept': 'application/json'}

usf_ana1_params = {
    'lat': -1.340332,
    'lon': -48.368021,
    'area_modulo': 2.012016,
    'nro_modulos': 328,
    'eficiencia_modulo': 0.1988,
    'capacidade': 131.2,
    'device_id': 1
}

usf_sat_params = {
    'lat': -1.071451,
    'lon': -48.126467,
    'area_modulo': 2.556048,
    'nro_modulos': 204,
    'eficiencia_modulo': 0.215,
    'capacidade': 111.18,
    'device_id': 14
}

usf_buj_params = {
    'lat': -1.667214,
    'lon': -48.054720,
    'area_modulo': 2.578716,
    'nro_modulos': 612,
    'eficiencia_modulo': 0.2055,
    'capacidade': 324.36,
    'device_id': 0
}

usf_ana2_params = {
    'lat': -1.340250,
    'lon': -48.368181,
    'area_modulo': 2.173572,
    'nro_modulos': 228,
    'eficiencia_modulo': 0.207,
    'capacidade': 102.6,
    'device_id': 3
}

api_params = {
    'lat': -1.071451,
    'lon': -48.126467,
    'month': 9,
    'global': 1,
    'raddatabase': 'PVGIS-SARAH2',
    'localtime': 1,
    'angle': 10,
    'aspect': 180,
    'showtemperatures': 1,
    'outputformat': 'json'
}

if flag == True:
    try:
        res = req.get(api_url, params=api_params, headers=api_headers)
        res.raise_for_status()
        json_res = res.json()
        try:
            with open("sample.json", "w") as out_file:
                data = json.dump(json_res, out_file)
        except Exception as e:
            print(f"(ERRO) ao processar JSON: {e}")

    except req.exceptions.RequestException as e:
        raise SystemExit(e)
else:
    try:
        with open("sample.json", "r") as input_file:
            json_res = json.load(input_file)
            lista = encontrar_chave(json_res, 'daily_profile')
            try:
                lista_filtrada = [(chave['time'], chave['Gd(i)'])
                                  for chave in lista]
                # print(lista_filtrada)
            except KeyError:
                print("(ERRO) Algumas chaves não estão presentes nos dicionários.")
    except FileNotFoundError:
        print("(ERRO) Arquivo não encontrado.")
    except json.JSONDecodeError as e:
        print(f"(ERRO) ao decodificar JSON: {e}")

time_format = '%Y-%m-%d %H:%M:%S'

host = "localhost"
database = "uirapurudb-dump-prod"
user = "postgres"
password = "01042019"

try:
    # Cria uma conexão com o banco de dados
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )

    # Cria um cursor para executar a consulta
    cursor = connection.cursor()

    # Executa a consulta desejada
    query = """
    SELECT payload 
    FROM day_messages dm 
    WHERE device_id = 1 
    AND (created_at BETWEEN '2023-06-26' AND '2023-06-27') 
    ORDER BY created_at DESC
    """

    cursor.execute(query)

    # Recupera os resultados da consulta
    results = cursor.fetchall()

    # Fecha o cursor e a conexão com o banco de dados
    cursor.close()
    connection.close()

except psycopg2.Error as e:
    print("Erro ao conectar ao banco de dados:", e)
