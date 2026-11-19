import json
import time
import paho.mqtt.client as mqtt

BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "smartbus/stops/1/incoming"

# Dados baseados no seu SQL (ajuste se houver outros registros no banco)
FROTA_BANCO = [
    {
        "line": "Linha 101",
        "destination": "Terminal Central - Jardim Primavera",
        "vehicleId": "ABC1D23",
        "status": "DISPONIVEL",
        "intervaloMin": 15,
        "source": "banco_oracle",
    },
    {
        "line": "Linha 102",
        "destination": "Terminal Central - Vila Nova",
        "vehicleId": "DEF4G56",
        "status": "DISPONIVEL",
        "intervaloMin": 20,
        "source": "banco_oracle",
    },
    {
        "line": "Linha 105",
        "destination": "Terminal Norte - Centro",
        "vehicleId": "OPQ4R56",
        "status": "DISPONIVEL",
        "intervaloMin": 10,
        "source": "banco_oracle",
    },
]

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT, 60)
client.loop_start()

try:
    idx = 0
    while True:
        bus = FROTA_BANCO[idx % len(FROTA_BANCO)]
        for eta in [5, 4, 3, 2, 1]:
            payload = {
                "line": bus["line"],
                "destination": bus["destination"],
                "vehicleId": bus["vehicleId"],
                "etaMinutes": eta,
                "noService": False,
                "source": bus["source"],
            }
            client.publish(TOPIC, json.dumps(payload), qos=0, retain=False)
            print("Enviado ao MQTT:", payload)
            time.sleep(5)
        idx += 1
except KeyboardInterrupt:
    print("\nTeste encerrado.")
finally:
    client.loop_stop()
    client.disconnect()