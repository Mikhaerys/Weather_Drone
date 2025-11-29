"""
Script para leer datos de Firebase Realtime Database y guardarlos en SQLite.
Ahora también consulta la Google Maps Weather API (Current Conditions) para enriquecer
cada registro con variables meteorológicas adicionales útiles para un dataset de
predicción de lluvia.

Guarda sólo registros nuevos (según cambios en sensores o probabilidad de precipitación).
"""
import os
import sqlite3
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, Any

load_dotenv()

# Configuración de Firebase
FIREBASE_URL = os.getenv("FIREBASE_URL")
USER_UID = os.getenv("USER_UID")
DATABASE_PATH = f"UsersData/{USER_UID}"

# Credenciales de autenticación (de main.cpp)
FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY")
FIREBASE_USER_EMAIL = os.getenv("FIREBASE_USER_EMAIL")
FIREBASE_USER_PASSWORD = os.getenv("FIREBASE_USER_PASSWORD")

# Variable global para almacenar el ID token
_id_token = None
_token_expiry = 0

# Configuración de SQLite
SQLITE_DB = "weather_drone_data.db"

# Intervalo de consulta (en segundos)
QUERY_INTERVAL = 60  # Ajustar según necesidad

# Configuración Weather API (Google Maps Platform - Current Conditions)
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
WEATHER_UNITS_SYSTEM = "METRIC"  # METRIC | IMPERIAL
WEATHER_API_ENABLED = True  # Permitir desactivar rápidamente


WEATHER_COLUMNS = {
    'is_daytime': 'INTEGER',
    'dew_point': 'REAL',
    'heat_index': 'REAL',
    'wind_chill': 'REAL',
    'uv_index': 'INTEGER',
    'precipitation_probability_percent': 'INTEGER',
    'precipitation_probability_type': 'TEXT',
    'precip_qpf': 'REAL',
    'thunderstorm_probability': 'INTEGER',
    'air_pressure_msl': 'REAL',
    'wind_direction_degrees': 'INTEGER',
    'wind_direction_cardinal': 'TEXT',
    'wind_speed': 'REAL',
    'wind_gust': 'REAL',
    'visibility_distance': 'REAL',
    'cloud_cover': 'REAL',
    'feels_like_temperature': 'REAL'
}


def get_firebase_auth_token():
    """Obtiene un ID token de Firebase usando email y password.

    Utiliza la Firebase Auth REST API para autenticar al usuario.
    El token se almacena en caché y se reutiliza hasta que expire.
    """
    global _id_token, _token_expiry

    # Si tenemos un token válido, reutilizarlo
    if _id_token and time.time() < _token_expiry:
        return _id_token

    try:
        # Endpoint de autenticación de Firebase
        auth_url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_API_KEY}"

        payload = {
            "email": FIREBASE_USER_EMAIL,
            "password": FIREBASE_USER_PASSWORD,
            "returnSecureToken": True
        }

        response = requests.post(auth_url, json=payload, timeout=10)

        if response.status_code == 200:
            data = response.json()
            _id_token = data.get('idToken')
            # El token expira en 1 hora (3600 segundos), renovar antes
            _token_expiry = time.time() + 3300  # 55 minutos
            print("✅ Autenticación con Firebase exitosa")
            return _id_token
        else:
            error_data = response.json()
            error_msg = error_data.get('error', {}).get(
                'message', 'Unknown error')
            print(f"❌ Error de autenticación Firebase: {error_msg}")
            return None
    except Exception as e:
        print(f"❌ Error al autenticar con Firebase: {e}")
        return None


def init_database():
    """Inicializa la base de datos SQLite con las tablas necesarias y agrega columnas nuevas si faltan."""
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()

    # Crear tabla para los registros
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            temperature REAL,
            humidity REAL,
            pressure REAL,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            speed REAL,
            hdop REAL,
            satellites INTEGER,
            time_utc TEXT,
            rained INTEGER,
            rain_checked_at DATETIME
        )
    """)

    # Crear tabla para el último registro procesado
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS last_reading (
            id INTEGER PRIMARY KEY,
            temperature REAL,
            humidity REAL,
            pressure REAL,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            speed REAL,
            hdop REAL,
            satellites INTEGER,
            time_utc TEXT,
            rained INTEGER,
            rain_checked_at DATETIME,
            last_update DATETIME
        )
    """)

    # Insertar fila inicial si no existe
    cursor.execute("SELECT COUNT(*) FROM last_reading")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO last_reading (id) VALUES (1)")

    # Asegurar columnas de Weather API (migración suave)
    def ensure_columns(table: str, columns: Dict[str, str]):
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cursor.fetchall()}
        for col, col_type in columns.items():
            if col not in existing:
                try:
                    cursor.execute(
                        f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                except Exception as e:
                    print(
                        f"⚠️  No se pudo agregar columna '{col}' en '{table}': {e}")

    ensure_columns('weather_readings', WEATHER_COLUMNS)
    ensure_columns('last_reading', WEATHER_COLUMNS)

    conn.commit()
    conn.close()
    print(f"✅ Base de datos '{SQLITE_DB}' inicializada correctamente")


def get_firebase_data():
    """Obtiene los datos actuales de Firebase usando autenticación"""
    try:
        # Obtener token de autenticación
        auth_token = get_firebase_auth_token()
        if not auth_token:
            print("❌ No se pudo obtener token de autenticación")
            return None

        # Agregar el token de autenticación a la URL
        url = f"{FIREBASE_URL}/{DATABASE_PATH}.json?auth={auth_token}"
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if data:
                return data
            else:
                print("⚠️  No hay datos en Firebase")
                return None
        elif response.status_code == 401:
            print("❌ Error 401: Token de autenticación inválido o expirado")
            # Forzar renovación del token en el próximo intento
            global _id_token, _token_expiry
            _id_token = None
            _token_expiry = 0
            return None
        else:
            print(f"❌ Error al obtener datos: {response.status_code}")
            print(f"   Respuesta: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return None


def get_last_reading():
    """Obtiene el último registro guardado en SQLite incluyendo columnas meteorológicas externas."""
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            temperature, humidity, pressure, latitude, longitude,
            altitude, speed, hdop, satellites, time_utc, rained, rain_checked_at,
            is_daytime, dew_point, heat_index, wind_chill, uv_index,
            precipitation_probability_percent, precipitation_probability_type,
            precip_qpf, thunderstorm_probability, air_pressure_msl,
            wind_direction_degrees, wind_direction_cardinal, wind_speed,
            wind_gust, visibility_distance, cloud_cover, feels_like_temperature
        FROM last_reading WHERE id = 1
    """)

    result = cursor.fetchone()
    conn.close()

    if result and result[0] is not None:
        keys = [
            'temperature', 'humidity', 'pressure', 'latitude', 'longitude',
            'altitude', 'speed', 'hdop', 'satellites', 'time_utc', 'rained', 'rain_checked_at',
            'is_daytime', 'dew_point', 'heat_index', 'wind_chill', 'uv_index',
            'precipitation_probability_percent', 'precipitation_probability_type',
            'precip_qpf', 'thunderstorm_probability', 'air_pressure_msl',
            'wind_direction_degrees', 'wind_direction_cardinal', 'wind_speed',
            'wind_gust', 'visibility_distance', 'cloud_cover', 'feels_like_temperature'
        ]
        return dict(zip(keys, result))
    return None


def is_new_reading(firebase_data, last_reading):
    """Verifica si los datos son nuevos comparando con el último registro.

    Criterios:
    - Cambios en sensores base (temp/hum/pres o posición)
    - Cambio en timestamp timeUTC
    - Cambio en probabilidad de precipitación externa (si disponible)
    """
    if last_reading is None:
        return True

    # Comparar los valores clave para determinar si es un registro nuevo
    # Usamos tolerancia para valores float por precisión
    tolerance = 0.001

    try:
        # Comparar valores del BME280
        temp_diff = abs(firebase_data.get('temperature', 0) -
                        last_reading.get('temperature', 0))
        hum_diff = abs(firebase_data.get('humidity', 0) -
                       last_reading.get('humidity', 0))
        pres_diff = abs(firebase_data.get('pressure', 0) -
                        last_reading.get('pressure', 0))

        # Comparar valores GPS (si existen)
        lat_diff = abs(firebase_data.get('latitude', 0) -
                       last_reading.get('latitude', 0))
        lng_diff = abs(firebase_data.get('longitude', 0) -
                       last_reading.get('longitude', 0))

        # Si algún valor cambió significativamente, es un nuevo registro
        if (temp_diff > tolerance or hum_diff > tolerance or pres_diff > tolerance or
                lat_diff > tolerance or lng_diff > tolerance):
            return True

        # Comparar el timestamp UTC si existe
        if firebase_data.get('timeUTC') != last_reading.get('time_utc'):
            return True

        # Verificar cambio en probabilidad de precipitación (Weather API)
        new_precip_prob = firebase_data.get(
            'precipitation_probability_percent')
        old_precip_prob = last_reading.get('precipitation_probability_percent')
        if new_precip_prob is not None and old_precip_prob is not None:
            if new_precip_prob != old_precip_prob:
                return True

        return False
    except Exception as e:
        print(f"⚠️  Error al comparar datos: {e}")
        return True  # En caso de error, considerarlo nuevo


def save_to_sqlite(data):
    """Guarda los datos en SQLite (sensores + Weather API)."""
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()

    try:
        # Insertar nuevo registro
        cursor.execute("""
            INSERT INTO weather_readings (
                temperature, humidity, pressure, latitude, longitude, altitude,
                speed, hdop, satellites, time_utc, rained, rain_checked_at,
                is_daytime, dew_point, heat_index, wind_chill, uv_index,
                precipitation_probability_percent, precipitation_probability_type,
                precip_qpf, thunderstorm_probability, air_pressure_msl,
                wind_direction_degrees, wind_direction_cardinal, wind_speed,
                wind_gust, visibility_distance, cloud_cover, feels_like_temperature
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get('temperature'),
            data.get('humidity'),
            data.get('pressure'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('altitude'),
            data.get('speed'),
            data.get('hdop'),
            data.get('satellites'),
            data.get('timeUTC'),
            data.get('rained'),
            data.get('rain_checked_at'),
            data.get('is_daytime'),
            data.get('dew_point'),
            data.get('heat_index'),
            data.get('wind_chill'),
            data.get('uv_index'),
            data.get('precipitation_probability_percent'),
            data.get('precipitation_probability_type'),
            data.get('precip_qpf'),
            data.get('thunderstorm_probability'),
            data.get('air_pressure_msl'),
            data.get('wind_direction_degrees'),
            data.get('wind_direction_cardinal'),
            data.get('wind_speed'),
            data.get('wind_gust'),
            data.get('visibility_distance'),
            data.get('cloud_cover'),
            data.get('feels_like_temperature')
        ))

        # Actualizar el último registro
        cursor.execute("""
            UPDATE last_reading SET
                temperature = ?, humidity = ?, pressure = ?, latitude = ?, longitude = ?,
                altitude = ?, speed = ?, hdop = ?, satellites = ?, time_utc = ?,
                rained = ?, rain_checked_at = ?, last_update = ?,
                is_daytime = ?, dew_point = ?, heat_index = ?, wind_chill = ?, uv_index = ?,
                precipitation_probability_percent = ?, precipitation_probability_type = ?,
                precip_qpf = ?, thunderstorm_probability = ?, air_pressure_msl = ?,
                wind_direction_degrees = ?, wind_direction_cardinal = ?, wind_speed = ?,
                wind_gust = ?, visibility_distance = ?, cloud_cover = ?, feels_like_temperature = ?
            WHERE id = 1
        """, (
            data.get('temperature'), data.get(
                'humidity'), data.get('pressure'),
            data.get('latitude'), data.get('longitude'), data.get('altitude'),
            data.get('speed'), data.get('hdop'), data.get(
                'satellites'), data.get('timeUTC'),
            data.get('rained'), data.get(
                'rain_checked_at'), datetime.now().isoformat(),
            data.get('is_daytime'), data.get('dew_point'), data.get(
                'heat_index'), data.get('wind_chill'), data.get('uv_index'),
            data.get('precipitation_probability_percent'), data.get(
                'precipitation_probability_type'),
            data.get('precip_qpf'), data.get(
                'thunderstorm_probability'), data.get('air_pressure_msl'),
            data.get('wind_direction_degrees'), data.get(
                'wind_direction_cardinal'), data.get('wind_speed'),
            data.get('wind_gust'), data.get('visibility_distance'), data.get(
                'cloud_cover'), data.get('feels_like_temperature')
        ))

        conn.commit()
        print(
            f"✅ Nuevo registro guardado - Temp: {data.get('temperature')}°C, Hum: {data.get('humidity')}%, Pres: {data.get('pressure')} hPa")
        if data.get('precipitation_probability_percent') is not None:
            print(
                f"   🌧️ Prob. precipitación: {data.get('precipitation_probability_percent')}% ({data.get('precipitation_probability_type')}) QPF:{data.get('precip_qpf')}")
        if data.get('thunderstorm_probability') is not None:
            print(
                f"   ⛈️ Prob. tormenta: {data.get('thunderstorm_probability')}%")
        if data.get('cloud_cover') is not None:
            print(f"   ☁️ Nubosidad: {data.get('cloud_cover')}%")
        if data.get('wind_speed') is not None:
            print(
                f"   💨 Viento: {data.get('wind_speed')} km/h Dir:{data.get('wind_direction_cardinal')} Gust:{data.get('wind_gust')}")

        if data.get('latitude') and data.get('longitude'):
            print(f"   📍 GPS: Lat {data.get('latitude'):.6f}, Lng {data.get('longitude'):.6f}, "
                  f"Satellites: {data.get('satellites')}")

    except sqlite3.IntegrityError:
        print("Registro duplicado ignorado")
    except Exception as e:
        print(f"❌ Error al guardar: {e}")
    finally:
        conn.close()


def get_weather_api_data(lat: Optional[float], lng: Optional[float]) -> Optional[Dict[str, Any]]:
    """Consulta la Weather API de Google para condiciones actuales.

    Retorna diccionario con claves normalizadas o None si falla.
    """
    if not WEATHER_API_ENABLED:
        return None
    if WEATHER_API_KEY == "REPLACE_WITH_YOUR_GOOGLE_WEATHER_API_KEY":
        print("⚠️  Debes configurar WEATHER_API_KEY para obtener datos meteorológicos externos")
        return None
    if lat is None or lng is None:
        return None
    url = (
        f"https://weather.googleapis.com/v1/currentConditions:lookup?key={WEATHER_API_KEY}&location.latitude={lat}&location.longitude={lng}&unitsSystem={WEATHER_UNITS_SYSTEM}"
    )
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️  Weather API status {resp.status_code}")
            return None
        w = resp.json()
        result = {
            'is_daytime': w.get('isDaytime'),
            'dew_point': w.get('dewPoint', {}).get('degrees'),
            'heat_index': w.get('heatIndex', {}).get('degrees'),
            'wind_chill': w.get('windChill', {}).get('degrees'),
            'uv_index': w.get('uvIndex'),
            'precipitation_probability_percent': (w.get('precipitation', {}).get('probability', {}) or {}).get('percent'),
            'precipitation_probability_type': (w.get('precipitation', {}).get('probability', {}) or {}).get('type'),
            'precip_qpf': (w.get('precipitation', {}).get('qpf', {}) or {}).get('quantity'),
            'thunderstorm_probability': w.get('thunderstormProbability'),
            'air_pressure_msl': (w.get('airPressure', {}) or {}).get('meanSeaLevelMillibars'),
            'wind_direction_degrees': (w.get('wind', {}).get('direction', {}) or {}).get('degrees'),
            'wind_direction_cardinal': (w.get('wind', {}).get('direction', {}) or {}).get('cardinal'),
            'wind_speed': (w.get('wind', {}).get('speed', {}) or {}).get('value'),
            'wind_gust': (w.get('wind', {}).get('gust', {}) or {}).get('value'),
            'visibility_distance': (w.get('visibility', {}) or {}).get('distance'),
            'cloud_cover': w.get('cloudCover'),
            'feels_like_temperature': (w.get('feelsLikeTemperature', {}) or {}).get('degrees')
        }
        return result
    except Exception as e:
        print(f"⚠️  Error Weather API: {e}")
        return None


def get_total_records():
    """Obtiene el número total de registros en la base de datos"""
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_readings")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def main():
    """Función principal del script"""
    print("=" * 60)
    print("🌤️  Weather Drone - Firebase to SQLite Logger")
    print("=" * 60)
    print()

    # Inicializar base de datos
    init_database()
    print()

    # Autenticar con Firebase
    print("🔐 Autenticando con Firebase...")
    token = get_firebase_auth_token()
    if not token:
        print("❌ No se pudo autenticar. Verifica las credenciales.")
        return
    print()

    # Verificar configuración
    if USER_UID == "REPLACE_WITH_YOUR_USER_UID":
        print("⚠️  IMPORTANTE: Debes reemplazar USER_UID con tu ID de usuario")
        print("   Puedes obtenerlo del Serial Monitor cuando el ESP32 se conecte")
        print()

    print(f"🔄 Consultando Firebase cada {QUERY_INTERVAL} segundos...")
    print(f"📊 Base de datos SQLite: {SQLITE_DB}")
    print(f"🔗 Firebase URL: {FIREBASE_URL}/{DATABASE_PATH}")
    print()
    print("Presiona Ctrl+C para detener")
    print("-" * 60)

    try:
        while True:
            # Obtener datos de Firebase
            firebase_data = get_firebase_data()

            if firebase_data:
                # Obtener último registro de SQLite
                last_reading = get_last_reading()

                # Enriquecer con Weather API si hay coordenadas
                weather_extra = get_weather_api_data(
                    firebase_data.get(
                        'latitude'), firebase_data.get('longitude')
                )
                if weather_extra:
                    firebase_data.update(weather_extra)

                # Verificar si es un registro nuevo
                if is_new_reading(firebase_data, last_reading):
                    save_to_sqlite(firebase_data)
                    total = get_total_records()
                    print(f"   📈 Total de registros en base de datos: {total}")
                else:
                    print("⏭️  Sin cambios - registro ignorado")

            # Esperar antes de la próxima consulta
            time.sleep(QUERY_INTERVAL)

    except KeyboardInterrupt:
        print()
        print("-" * 60)
        print("🛑 Script detenido por el usuario")
        total = get_total_records()
        print(f"📊 Total de registros guardados: {total}")
        print("=" * 60)


if __name__ == "__main__":
    main()
