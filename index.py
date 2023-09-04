import requests as req
import json

# formato requerido URL : https://re.jrc.ec.europa.eu/api/v5_2/DRcalc?lat=-1.0714&lon=-48.1264&month=9&global=1&raddatabase=PVGIS-SARAH2&localtime=1&angle=10&aspect=180&showtemperatures=1&outputformat=json

api_url = 'https://re.jrc.ec.europa.eu/api/v5_2/DRcalc'

api_headers = {'Accept': 'application/json'}

api_params = {
    'lat': -1.1071,
    'lon': -48.1264,
    'month': 9,
    'global': 1,
    'raddatabase': 'PVGIS-SARAH2',
    'localtime': 1,
    'angle': 10,
    'aspect': 180,
    'showtemperatures': 1,
    'outputformat': 'json'
}

try:
    res = req.get(api_url, params=api_params, headers=api_headers)
    res.raise_for_status()
    json_res = res.json()

    try:
        with open ("sample.json", "w") as out_file:
            data = json.dump(json_res, out_file)
    except Exception as e:
        print(f"Erro ao processar JSON: {e}")

except req.exceptions.RequestException as e:
    raise SystemExit(e)
