import psycopg2
from datetime import datetime
import psycopg2.extras
import requests as req
import json

################## MEXER APENAS AQUI ###################

# POSSIBILIDADES : USF_ANA1, USF_ANA2, USF_BUJ, USF_SAT
usina = 'USF_SAT'

dia_para_calculo = '2023-09-03'

# acesso ao banco de dados
host = "localhost"
database = "uirapurudb-dump-prod"
user = "postgres"
password = "01042019"

##########################################################

formato_str_dia_do_ano = '%Y-%m-%d'
formato_str_tempo = '%Y-%m-%d %H:%M:%S'

# objetos datetime para query ao banco de dados
data_inicial = datetime.strptime(
    (dia_para_calculo + ' 06:00:00'), formato_str_tempo)
data_final = datetime.strptime(
    (dia_para_calculo + ' 18:59:00'), formato_str_tempo)

# objeto datetime para api de irradiancia
datetime_obj = datetime.strptime(dia_para_calculo, formato_str_dia_do_ano)

# mes para obter curva de tendencia de irradiancia da api de irradiancia
mes = datetime_obj.month


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
    'azimute': 225,
    'device_id': 1
}

usf_sat_params = {
    'lat': -1.071451,
    'lon': -48.126467,
    'area_modulo': 2.556048,
    'nro_modulos': 204,
    'eficiencia_modulo': 0.215,
    'capacidade': 111.18,
    'azimute': 180,
    'device_id': 14
}

usf_buj_params = {
    'lat': -1.667214,
    'lon': -48.054720,
    'area_modulo': 2.578716,
    'nro_modulos': 612,
    'eficiencia_modulo': 0.2055,
    'capacidade': 324.36,
    'azimute': 180,
    'device_id': 0
}

usf_ana2_params = {
    'lat': -1.340250,
    'lon': -48.368181,
    'area_modulo': 2.173572,
    'nro_modulos': 228,
    'eficiencia_modulo': 0.207,
    'capacidade': 102.6,
    'azimute': 225,
    'device_id': 3
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
    'aspect': usinas[usina]['azimute'],
    'showtemperatures': 1,
    'outputformat': 'json'
}

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

lista = encontrar_chave(json_res, 'daily_profile')
try:
    lista_filtrada = [{'data_hora': datetime.strptime(datetime_obj.strftime('%Y-%m-%d') + ' ' + chave['time'], '%Y-%m-%d %H:%M'), 'irrad': chave['Gb(i)']}
                      for chave in lista]
except KeyError:
    print("(ERRO) Algumas chaves não estão presentes nos dicionários.")

hora_inicio = datetime.strptime(datetime_obj.strftime(
    '%Y-%m-%d') + ' 06:00', '%Y-%m-%d %H:%M')
hora_fim = datetime.strptime(datetime_obj.strftime(
    '%Y-%m-%d') + ' 18:00', '%Y-%m-%d %H:%M')

# Verificar e remover as chaves antes e depois das chaves de interesse
inicio_encontrado = False
lista_tendencia_irradiancia_mes = []
for dicionario in lista_filtrada:
    if hora_inicio == dicionario['data_hora']:
        inicio_encontrado = True
    if inicio_encontrado:
        lista_tendencia_irradiancia_mes.append(dicionario)
        if hora_fim == dicionario['data_hora']:
            break
del lista, lista_filtrada

try:
    # Cria uma conexão com o banco de dados
    conexao = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )

    # Cria um cursor para executar a consulta
    cursor = conexao.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Consulta de potencia maxima nas horas do dia
    query = """
    SELECT 
        CAST(MAX(((m.payload::jsonb)->>'pt')::NUMERIC) AS NUMERIC(10,2))  AS "Potência maxima(W)",
        reference_time AS "Hora"
    FROM 
	    messages m
    JOIN
    (
        SELECT 
        DISTINCT
        	date_trunc('hour', selected_time) AS reference_time,
            date_trunc('hour', selected_time) - INTERVAL '%(meio_intervalo)s minutes' AS start_time,
            date_trunc('hour', selected_time) + INTERVAL '%(meio_intervalo)s minutes' AS end_time
        FROM
            generate_series(
                %(data_inicial)s::timestamp, -- Hora inicial
                %(data_final)s::timestamp, -- Hora final
                '%(intervalo)s minutes'::interval -- Intervalo de iteração
            ) AS selected_time
    ) d
    ON 
    m.created_at >= d.start_time
    AND m.created_at <= d.end_time
    WHERE 
    device_id = %(device_id)s
    GROUP BY
	d.reference_time
    ORDER BY reference_time ASC;
    """

    cursor.execute(query, {'meio_intervalo': 5, 'data_inicial': data_inicial.strftime(
        formato_str_tempo), 'data_final': data_final.strftime(formato_str_tempo), 'intervalo': 10, 'device_id': usinas[usina]['device_id']})

    lista_potencia_dia = []
    # Recupera os resultados da consulta
    resultados = cursor.fetchall()
    for linha in resultados:
        dicionario = {'data_hora': 0, 'pot': 0}
        for dado in linha:
            if (type(dado) == datetime):
                dicionario['data_hora'] = dado
            else:
                dicionario["pot"] = float(dado)
        lista_potencia_dia.append(dicionario)

    # Consulta da produção diária
    query2 = """
    SELECT
	    CAST(value / 1000 AS NUMERIC(12,3)) AS "producao_diaria(kWh)"
    FROM
	    energy
    WHERE
	    device_id = %(device_id)s
	AND "date" = %(dia)s
	AND "period" = 'day';
    """

    cursor.execute(
        query2, {'device_id': usinas[usina]['device_id'], 'dia': dia_para_calculo})
    producao_diaria = cursor.fetchall()

    # Fecha o cursor e a conexão com o banco de dados
    cursor.close()
    conexao.close()

except psycopg2.Error as e:
    print("Erro ao conectar ao banco de dados:", e)

lista_irrad = []
lista_eficiencia_hora = []
if (len(lista_potencia_dia) == len(lista_tendencia_irradiancia_mes)):
    for item1, item2 in zip(lista_potencia_dia, lista_tendencia_irradiancia_mes):
        lista_irrad.append(item2['irrad'])
        if item1['data_hora'] == item2['data_hora']:
            denominador = item2['irrad']*usinas[usina]['area_modulo'] * \
                usinas[usina]['nro_modulos']*usinas[usina]['eficiencia_modulo']
            numerador = 100 * item1['pot']
            if denominador != 0:
                eficiencia = float(numerador/denominador)
            else:
                eficiencia = 0
            lista_eficiencia_hora.append(eficiencia)

lista_producao_diaria = []
for linha in producao_diaria:
    for conteudo in linha:
        lista_producao_diaria.append((float(conteudo)))

print(lista_producao_diaria)
print(lista_eficiencia_hora)

# calcula a eficiencia diaria como a media das eficiencias por hora
media_eficiencia_hora = sum(lista_eficiencia_hora) / len(lista_eficiencia_hora)

#calcula a eficiencia diaria por meio da produção
eficiencia_diaria_metodo_2 = max(lista_producao_diaria)*100/(usinas[usina]['capacidade']*(sum(lista_irrad)/1000))

print(f'Média de eficiência por hora: {media_eficiencia_hora}%')

print(f'Eficiênica calculada: {eficiencia_diaria_metodo_2}%')