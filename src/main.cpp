#define ENABLE_USER_AUTH
#define ENABLE_DATABASE

#include <Arduino.h>
#if defined(ESP32)
#include <WiFi.h>
#elif defined(ESP8266)
#include <ESP8266WiFi.h>
#endif
#include <WiFiClientSecure.h>
#include <FirebaseClient.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <TinyGPS++.h>

// Network and Firebase credentials
#define WIFI_SSID "REPLACE_WITH_YOUR_SSID"
#define WIFI_PASSWORD "REPLACE_WITH_YOUR_PASSWORD"

#define Web_API_KEY "REPLACE_WITH_YOUR_PROJECT_API_KEY"
#define DATABASE_URL "REPLACE_WITH_YOUR_DATABASE_URL"
#define USER_EMAIL "REPLACE_WITH_THE_USER_EMAIL"
#define USER_PASSWORD "REPLACE_WITH_THE_USER_PASSWORD"

// GPS Configuration
#define RXD2 16
#define TXD2 17
#define GPS_BAUD 9600

// User function
void processData(AsyncResult &aResult);

// Authentication
UserAuth user_auth(Web_API_KEY, USER_EMAIL, USER_PASSWORD);

// Firebase components
FirebaseApp app;
WiFiClientSecure ssl_client;
using AsyncClient = AsyncClientClass;
AsyncClient aClient(ssl_client);
RealtimeDatabase Database;

// Timer variables for sending data every 10 seconds
unsigned long lastSendTime = 0;
const unsigned long sendInterval = 10000;

// Variable to save USER UID
String uid;

// Variables to save database paths
String databasePath;
String tempPath;
String humPath;
String presPath;
String latPath;
String lngPath;
String altPath;
String speedPath;
String hdopPath;
String satellitesPath;
String timeUTCPath;

// BME280 sensor
Adafruit_BME280 bme; // I2C
float temperature;
float humidity;
float pressure;

// GPS objects
TinyGPSPlus gps;
HardwareSerial gpsSerial(2);
double latitude;
double longitude;
double altitude;
double speed;
double hdop;
int satellites;
String timeUTC;

// Initialize BME280
void initBME()
{
    if (!bme.begin(0x76))
    {
        Serial.println("Could not find a valid BME280 sensor, check wiring!");
        while (1)
            ;
    }
    Serial.println("BME280 Initialized with success");
}

// Initialize GPS
void initGPS()
{
    gpsSerial.begin(GPS_BAUD, SERIAL_8N1, RXD2, TXD2);
    Serial.println("GPS Serial started at 9600 baud rate");
}

void setup()
{
    Serial.begin(115200);

    initBME();
    initGPS();

    // Connect to Wi-Fi
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    Serial.print("Connecting to Wi-Fi");
    while (WiFi.status() != WL_CONNECTED)
    {
        Serial.print(".");
        delay(300);
    }
    Serial.println();

    ssl_client.setInsecure();
#if defined(ESP32)
    ssl_client.setHandshakeTimeout(5);
#elif defined(ESP8266)
    ssl_client.setTimeout(1000);           // Set connection timeout
    ssl_client.setBufferSizes(4096, 1024); // Set buffer sizes
#endif

    // Initialize Firebase
    initializeApp(aClient, app, getAuth(user_auth), processData, "🔐 authTask");
    app.getApp<RealtimeDatabase>(Database);
    Database.url(DATABASE_URL);
}

void loop()
{
    // Maintain authentication and async tasks
    app.loop();

    // Read GPS data
    while (gpsSerial.available() > 0)
    {
        gps.encode(gpsSerial.read());
    }

    // Check if authentication is ready
    if (app.ready())
    {

        // Periodic data sending every 10 seconds
        unsigned long currentTime = millis();
        if (currentTime - lastSendTime >= sendInterval)
        {
            // Update the last send time
            lastSendTime = currentTime;

            // Get User UID
            Firebase.printf("User UID: %s\n", app.getUid().c_str());
            uid = app.getUid().c_str();
            databasePath = "UsersData/" + uid;

            // Update database path for sensor readings
            tempPath = databasePath + "/temperature";      // --> UsersData/<user_uid>/temperature
            humPath = databasePath + "/humidity";          // --> UsersData/<user_uid>/humidity
            presPath = databasePath + "/pressure";         // --> UsersData/<user_uid>/pressure
            latPath = databasePath + "/latitude";          // --> UsersData/<user_uid>/latitude
            lngPath = databasePath + "/longitude";         // --> UsersData/<user_uid>/longitude
            altPath = databasePath + "/altitude";          // --> UsersData/<user_uid>/altitude
            speedPath = databasePath + "/speed";           // --> UsersData/<user_uid>/speed
            hdopPath = databasePath + "/hdop";             // --> UsersData/<user_uid>/hdop
            satellitesPath = databasePath + "/satellites"; // --> UsersData/<user_uid>/satellites
            timeUTCPath = databasePath + "/timeUTC";       // --> UsersData/<user_uid>/timeUTC

            // Get latest sensor readings
            temperature = bme.readTemperature();
            humidity = bme.readHumidity();
            pressure = bme.readPressure() / 100.0F;

            // Get GPS data if available
            if (gps.location.isValid())
            {
                latitude = gps.location.lat();
                longitude = gps.location.lng();
                altitude = gps.altitude.meters();
                speed = gps.speed.kmph();
                hdop = gps.hdop.value() / 100.0;
                satellites = gps.satellites.value();

                // Format UTC time
                timeUTC = String(gps.date.year()) + "/" +
                          String(gps.date.month()) + "/" +
                          String(gps.date.day()) + "," +
                          String(gps.time.hour()) + ":" +
                          String(gps.time.minute()) + ":" +
                          String(gps.time.second());

                Serial.print("LAT: ");
                Serial.println(latitude, 6);
                Serial.print("LONG: ");
                Serial.println(longitude, 6);
                Serial.print("SPEED (km/h) = ");
                Serial.println(speed);
                Serial.print("ALT (m) = ");
                Serial.println(altitude);
                Serial.print("HDOP = ");
                Serial.println(hdop);
                Serial.print("Satellites = ");
                Serial.println(satellites);
                Serial.print("Time in UTC: ");
                Serial.println(timeUTC);
                Serial.println("");
            }
            else
            {
                Serial.println("GPS location not valid yet");
            }

            Serial.println("Writing to: " + tempPath);

            // Send BME280 data
            Database.set<float>(aClient, tempPath, temperature, processData, "RTDB_Send_Temperature");
            Database.set<float>(aClient, humPath, humidity, processData, "RTDB_Send_Humidity");
            Database.set<float>(aClient, presPath, pressure, processData, "RTDB_Send_Pressure");

            // Send GPS data if valid
            if (gps.location.isValid())
            {
                Database.set<double>(aClient, latPath, latitude, processData, "RTDB_Send_Latitude");
                Database.set<double>(aClient, lngPath, longitude, processData, "RTDB_Send_Longitude");
                Database.set<double>(aClient, altPath, altitude, processData, "RTDB_Send_Altitude");
                Database.set<double>(aClient, speedPath, speed, processData, "RTDB_Send_Speed");
                Database.set<double>(aClient, hdopPath, hdop, processData, "RTDB_Send_HDOP");
                Database.set<int>(aClient, satellitesPath, satellites, processData, "RTDB_Send_Satellites");
                Database.set<String>(aClient, timeUTCPath, timeUTC, processData, "RTDB_Send_TimeUTC");
            }
        }
    }
}

void processData(AsyncResult &aResult)
{
    if (!aResult.isResult())
        return;

    if (aResult.isEvent())
        Firebase.printf("Event task: %s, msg: %s, code: %d\n", aResult.uid().c_str(), aResult.eventLog().message().c_str(), aResult.eventLog().code());

    if (aResult.isDebug())
        Firebase.printf("Debug task: %s, msg: %s\n", aResult.uid().c_str(), aResult.debug().c_str());

    if (aResult.isError())
        Firebase.printf("Error task: %s, msg: %s, code: %d\n", aResult.uid().c_str(), aResult.error().message().c_str(), aResult.error().code());

    if (aResult.available())
        Firebase.printf("task: %s, payload: %s\n", aResult.uid().c_str(), aResult.c_str());
}