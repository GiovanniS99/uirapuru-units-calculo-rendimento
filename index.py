import requests as req
import json
import psycopg2

# POSSIBILIDADES : USF_ANA1, USF_ANA2, USF_BUJ, USF_SAT
usina = 'USF_ANA1'
mes = 9


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


usf_ana1_params = {
    'lat': -1.340332,
    'lon': -48.368021,
    'area_modulo': 2.012016,
    'nro_modulos': 328,
    'eficiencia_modulo': 0.1988,
    'capacidade': 131.2,
    'azimute': 225
}

usf_sat_params = {
    'lat': -1.071451,
    'lon': -48.126467,
    'area_modulo': 2.556048,
    'nro_modulos': 204,
    'eficiencia_modulo': 0.215,
    'capacidade': 111.18,
    'azimute': 180
}

usf_buj_params = {
    'lat': -1.667214,
    'lon': -48.054720,
    'area_modulo': 2.578716,
    'nro_modulos': 612,
    'eficiencia_modulo': 0.2055,
    'capacidade': 324.36,
    'azimute': 180
}

usf_ana2_params = {
    'lat': -1.340250,
    'lon': -48.368181,
    'area_modulo': 2.173572,
    'nro_modulos': 228,
    'eficiencia_modulo': 0.207,
    'capacidade': 102.6,
    'device_id': 225
}

usinas = {'USF_ANA1': usf_ana1_params, 'USF_SAT': usf_sat_params,
          'USF_BUJ': usf_buj_params, 'USF_ANA2': usf_ana2_params}
del usf_ana1_params, usf_sat_params, usf_buj_params, usf_ana2_params

api_params = {
    'lat': usinas[usina]['lat'],
    'lon': usinas[usina]['lon'],
    'month': mes,
    'global': 1,
    'raddatabase': 'PVGIS-SARAH2',
    'localtime': 1,
    'angle': 10,
    'aspect': 180,
    'showtemperatures': 1,
    'outputformat': 'json'
}

# formato requerido URL : https://re.jrc.ec.europa.eu/api/v5_2/DRcalc?lat=-1.0714&lon=-48.1264&month=9&global=1&raddatabase=PVGIS-SARAH2&localtime=1&angle=10&aspect=180&showtemperatures=1&outputformat=json

api_url = 'https://re.jrc.ec.europa.eu/api/v5_2/DRcalc'

api_headers = {'Accept': 'application/json'}

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

time_format = '%Y-%m-%d %H:%M:%S'

host = "localhost"
database = "uirapurudb-dump-prod"
user = "postgres"
password = "Giogears99"

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
