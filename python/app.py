import os
import io
import csv
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, redirect, url_for, render_template_string, flash, Response

load_dotenv()
SQLITE_DB = "weather_drone_data.db"

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")

DEFAULT_PAGE_SIZE = 25

TABLE_COLUMNS = [
    'id', 'timestamp', 'temperature', 'humidity', 'pressure', 'latitude', 'longitude', 'altitude', 'speed', 'hdop', 'satellites', 'time_utc', 'rained', 'rain_checked_at',
    'is_daytime', 'dew_point', 'heat_index', 'wind_chill', 'uv_index', 'precipitation_probability_percent', 'precipitation_probability_type', 'precip_qpf', 'thunderstorm_probability', 'air_pressure_msl', 'wind_direction_degrees', 'wind_direction_cardinal', 'wind_speed', 'wind_gust', 'visibility_distance', 'cloud_cover', 'feels_like_temperature'
]

BASE_QUERY = f"SELECT {', '.join(TABLE_COLUMNS)} FROM weather_readings"


def get_connection():
    return sqlite3.connect(SQLITE_DB)


def fetch_page(page: int, page_size: int, only_unlabeled: bool = False):
    offset = (page-1)*page_size
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    where = "WHERE rained IS NULL" if only_unlabeled else ""
    cur.execute(
        f"{BASE_QUERY} {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?", (page_size, offset))
    rows = cur.fetchall()
    if only_unlabeled:
        cur.execute(
            "SELECT COUNT(*) FROM weather_readings WHERE rained IS NULL")
    else:
        cur.execute("SELECT COUNT(*) FROM weather_readings")
    total = cur.fetchone()[0]
    conn.close()
    return rows, total


def update_rained(records):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    updated = 0
    for rec_id, val in records.items():
        if val not in ("0", "1", "", None):
            continue
        if val == "":
            cur.execute(
                "UPDATE weather_readings SET rained=NULL, rain_checked_at=NULL WHERE id=?", (rec_id,))
        else:
            cur.execute("UPDATE weather_readings SET rained=?, rain_checked_at=? WHERE id=?", (int(
                val), now, rec_id))
        updated += 1
    conn.commit()
    conn.close()
    return updated


@app.route('/')
def index():
    try:
        page = int(request.args.get('page', '1'))
    except ValueError:
        page = 1
    try:
        page_size = int(request.args.get('page_size', str(DEFAULT_PAGE_SIZE)))
    except ValueError:
        page_size = DEFAULT_PAGE_SIZE
    only_unlabeled = request.args.get('filter') == 'unlabeled'
    rows, total = fetch_page(page, page_size, only_unlabeled)
    total_pages = (total + page_size - 1)//page_size
    return render_template_string(TEMPLATE_INDEX,
                                  rows=rows,
                                  page=page,
                                  page_size=page_size,
                                  total=total,
                                  total_pages=total_pages,
                                  only_unlabeled=only_unlabeled
                                  )


@app.route('/update', methods=['POST'])
def update():
    form_records = {}
    for key, value in request.form.items():
        if key.startswith('rained_'):
            rec_id = key.split('_', 1)[1]
            form_records[rec_id] = value
    updated = update_rained(form_records)
    flash(f"Actualizados {updated} registros")
    return redirect(request.referrer or url_for('index'))


@app.route('/export.csv')
def export_csv():
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"{BASE_QUERY} ORDER BY timestamp ASC")
    rows = cur.fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(TABLE_COLUMNS)
    for r in rows:
        writer.writerow([r[c] for c in TABLE_COLUMNS])
    csv_data = output.getvalue()
    output.close()
    return Response(csv_data, mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=weather_dataset.csv'})


TEMPLATE_INDEX = """
<!DOCTYPE html>
<html lang='es'>
<head>
	<meta charset='UTF-8' />
	<title>Weather Drone Dataset Annotator</title>
	<style>
		body { font-family: Arial, sans-serif; margin: 20px; }
		table { border-collapse: collapse; width: 100%; font-size: 12px; }
		th, td { border: 1px solid #ccc; padding: 4px; text-align: center; }
		th { background: #f0f0f0; position: sticky; top:0; }
		.controls { margin-bottom: 12px; display:flex; gap:12px; flex-wrap:wrap; }
		.rained-select { width: 70px; }
		.flash { background:#dff0d8; padding:6px 10px; border:1px solid #b2d8b2; margin-bottom:10px; }
		nav a { margin:0 4px; text-decoration:none; }
		nav span { margin:0 4px; }
	</style>
</head>
<body>
	<h1>Weather Drone Dataset Annotator</h1>
	{% with messages = get_flashed_messages() %}
		{% if messages %}
			{% for m in messages %}<div class="flash">{{m}}</div>{% endfor %}
		{% endif %}
	{% endwith %}
	<div class='controls'>
		<form method='get'>
			<input type='hidden' name='filter' value='{{ 'unlabeled' if only_unlabeled else '' }}'>
			<label>Página: <input type='number' name='page' value='{{page}}' min='1'></label>
			<label>Tamaño: <input type='number' name='page_size' value='{{page_size}}' min='5'></label>
			<button type='submit'>Ir</button>
		</form>
		<form method='get'>
			{% if not only_unlabeled %}
				<input type='hidden' name='filter' value='unlabeled'>
				<button type='submit'>Mostrar sólo sin etiqueta</button>
			{% else %}
				<button type='submit'>Mostrar todos</button>
			{% endif %}
		</form>
		<a href='{{ url_for('export_csv') }}'>Exportar CSV</a>
	</div>
	<form method='post' action='{{ url_for('update') }}'>
		<table>
			<thead>
				<tr>
					<th>ID</th>
					<th>Timestamp</th>
					<th>Temp</th>
					<th>Hum</th>
					<th>Pres</th>
					<th>Lat</th>
					<th>Lng</th>
					<th>Alt</th>
					<th>Vel</th>
					<th>Sat</th>
					<th>UTC</th>
					<th>Rained</th>
					<th>DewPt</th>
					<th>HeatIdx</th>
					<th>WindChill</th>
					<th>UV</th>
					<th>ProbPrec %</th>
					<th>TipoPrec</th>
					<th>QPF</th>
					<th>ProbTormenta %</th>
					<th>Pres MSL</th>
					<th>Viento Dir</th>
					<th>Viento Vel</th>
					<th>Racha</th>
					<th>Nubosidad %</th>
				</tr>
			</thead>
			<tbody>
				{% for r in rows %}
				<tr>
					<td>{{r['id']}}</td>
					<td>{{r['timestamp']}}</td>
					<td>{{r['temperature']}}</td>
					<td>{{r['humidity']}}</td>
					<td>{{r['pressure']}}</td>
					<td>{{'%0.5f' % r['latitude'] if r['latitude'] is not none else ''}}</td>
					<td>{{'%0.5f' % r['longitude'] if r['longitude'] is not none else ''}}</td>
					<td>{{r['altitude']}}</td>
					<td>{{r['speed']}}</td>
					<td>{{r['satellites']}}</td>
					<td>{{r['time_utc']}}</td>
					<td>
						<select name='rained_{{r['id']}}' class='rained-select'>
							<option value='' {% if r['rained'] is none %}selected{% endif %}>?</option>
							<option value='1' {% if r['rained'] == 1 %}selected{% endif %}>Sí</option>
							<option value='0' {% if r['rained'] == 0 %}selected{% endif %}>No</option>
						</select>
					</td>
					<td>{{r['dew_point']}}</td>
					<td>{{r['heat_index']}}</td>
					<td>{{r['wind_chill']}}</td>
					<td>{{r['uv_index']}}</td>
					<td>{{r['precipitation_probability_percent']}}</td>
					<td>{{r['precipitation_probability_type']}}</td>
					<td>{{r['precip_qpf']}}</td>
					<td>{{r['thunderstorm_probability']}}</td>
					<td>{{r['air_pressure_msl']}}</td>
					<td>{{r['wind_direction_cardinal']}}</td>
					<td>{{r['wind_speed']}}</td>
					<td>{{r['wind_gust']}}</td>
					<td>{{r['cloud_cover']}}</td>
				</tr>
				{% endfor %}
			</tbody>
		</table>
		<p><button type='submit'>Guardar cambios</button></p>
	</form>
	<nav>
		{% for p in range(1, total_pages+1) %}
			{% if p == page %}<span><strong>{{p}}</strong></span>{% else %}<a href='?page={{p}}&page_size={{page_size}}{% if only_unlabeled %}&filter=unlabeled{% endif %}'>{{p}}</a>{% endif %}
		{% endfor %}
	</nav>
	<p>Total registros: {{total}} | Página {{page}} de {{total_pages}}</p>
</body>
</html>
"""

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
